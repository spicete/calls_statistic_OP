<?php
/**
 * Скрипт-демон для ежедневного сбора статистики звонков и отправки в чат Bitrix24.
 * Запускается ежедневно в 07:00 и 11:30 по МСК (соответствует 13:00 и 17:30 по Якутску, UTC+9).
 */

// Устанавливаем временную зону МСК
date_default_timezone_set('Europe/Moscow');

// Подключаем SDK CRest
require_once __DIR__ . '/crest.php';

// === ЛОГГЕР ===
class Logger {
    private string $filePath;
    private int $maxLines = 5000;
    private int $eventCounter = 0;
    private int $pid;

    public function __construct(string $dir) {
        if (!is_dir($dir)) {
            mkdir($dir, 0755, true);
        }
        $this->filePath = rtrim($dir, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . 'history_daily.log';
        $this->pid = getmypid();
    }

    public function log(string $direction, string $context): void {
        $this->eventCounter++;
        $timestamp = (new DateTime('now'))->format('Y-m-d\TH:i:sP');
        $dirRu = match(strtoupper($direction)) {
            'INCOMING' => 'ВХОДЯЩИЙ',
            'OUTGOING' => 'ИСХОДЯЩИЙ',
            default    => mb_strtoupper($direction),
        };
        $line = sprintf("%d %d %s %s %s\n",
            $this->eventCounter,
            $this->pid,
            $timestamp,
            $dirRu,
            $context
        );
        file_put_contents($this->filePath, $line, FILE_APPEND);

        // Обрезаем лог, если строк больше maxLines
        $lines = file($this->filePath, FILE_IGNORE_NEW_LINES);
        if (count($lines) > $this->maxLines) {
            $lines = array_slice($lines, -$this->maxLines);
            file_put_contents($this->filePath, implode("\n", $lines) . "\n");
        }
    }
}

// Получение всей истории звонков за период
function fetchRawCalls(string $fromDate, string $toDate, Logger $logger): array {
    $rawCalls = [];
    $start = 0;
    do {
        $params = [
            'FILTER' => [
                '>CALL_START_DATE' => $fromDate,
                '<CALL_START_DATE' => $toDate,
                'PORTAL_NUMBER'    => ['74112243067', 'reg114284', 73832349859, 79014699873],
            ],
            'SORT'  => ['CALL_START_DATE' => 'DESC'],
            'start' => $start,
        ];
        $logger->log('OUTGOING', "voximplant.statistic.get (start={$start})");
        $resp = CRest::call('voximplant.statistic.get', $params);
        if (isset($resp['error'])) {
            $logger->log('OUTGOING', 'Ошибка при получении звонков: ' . $resp['error_description']);
            throw new RuntimeException('Error fetching calls: ' . $resp['error_description']);
        }
        if (!empty($resp['result'])) {
            $rawCalls = array_merge($rawCalls, $resp['result']);
        }
        $start = isset($resp['next']) ? (int)$resp['next'] : 0;
    } while ($start > 0);

    $logger->log('INCOMING', 'Получено ' . count($rawCalls) . ' сырых записей звонков');
    return $rawCalls;
}

// Получение списка активных менеджеров
function fetchManagers(Logger $logger): array {
    $filter = [
        'ACTIVE'         => 'Y',
        '%WORK_POSITION' => 'хантер',
        'UF_DEPARTMENT'  => [47],
    ];
    $params = ['FILTER' => $filter, 'SELECT' => ['ID','NAME','LAST_NAME']];
    $logger->log('OUTGOING', 'user.get для получения менеджеров');
    $resp = CRest::call('user.get', $params);
    if (isset($resp['error'])) {
        $logger->log('OUTGOING', 'Ошибка при получении менеджеров: ' . $resp['error_description']);
        throw new RuntimeException('Error fetching managers: ' . $resp['error_description']);
    }
    $map = [];
    foreach ($resp['result'] as $m) {
        $map[$m['ID']] = trim("{$m['NAME']} {$m['LAST_NAME']}");
    }
    $logger->log('INCOMING', 'Получено ' . count($map) . ' менеджеров');
    return $map;
}

// Фильтрация звонков по менеджерам с нормализацией номера
function filterCalls(array $rawCalls, array $managerMap, Logger $logger): array {
    $filtered = [];
    foreach ($rawCalls as $call) {
        $uid = $call['PORTAL_USER_ID'] ?? null;
        if (!$uid || !isset($managerMap[$uid])) continue;

        // Нормализуем номер: оставляем только цифры, берём последние 10 и добавляем "7"
        $digitsOnly = preg_replace('/\D+/', '', $call['PHONE_NUMBER']);
        $last10     = substr($digitsOnly, -10);
        $normPhone  = '7' . $last10;

        $filtered[] = [
            'ID'              => $call['ID'],
            'USER_ID'         => $uid,
            'USER_NAME'       => $managerMap[$uid],
            'PORTAL_NUMBER'   => $call['PORTAL_NUMBER'],
            'PHONE_NUMBER'    => $normPhone,
            'CALL_DURATION'   => $call['CALL_DURATION'],
            'CALL_START_DATE' => $call['CALL_START_DATE'],
            'CALL_TYPE'       => $call['CALL_TYPE']==2 ? 'INCOMING' : 'OUTGOING',
            'CALL_FAILED_CODE'=> $call['CALL_FAILED_CODE'] ?? null,
        ];
    }
    usort($filtered, fn($a, $b) => strcmp($a['CALL_START_DATE'], $b['CALL_START_DATE']));
    $logger->log('INCOMING', 'Отфильтровано ' . count($filtered) . ' звонков по менеджерам');
    return $filtered;
}

// Вычисление статистики
function computeStatistics(array $calls, Logger $logger): array {
    $stats = [
        'totalCalls'     => 0,
        'totalIncoming'  => 0,
        'totalOutgoing'  => 0,
        'missedIncoming' => 0,
        'missedOutgoing' => 0,
        'employees'      => [],
    ];

    foreach ($calls as $call) {
        $name = $call['USER_NAME'];
        $type = $call['CALL_TYPE']; // 'INCOMING' | 'OUTGOING'
        $code = (string)($call['CALL_FAILED_CODE'] ?? '');

        if (!isset($stats['employees'][$name])) {
            $stats['employees'][$name] = [
                'total_calls'      => 0,
                'incoming'         => 0,
                'outgoing'         => 0,
                'missed_incoming'  => 0,
                'missed_outgoing'  => 0,
            ];
        }

        $stats['totalCalls']++;
        $stats['employees'][$name]['total_calls']++;

        if ($type === 'INCOMING') {
            $stats['totalIncoming']++;
            $stats['employees'][$name]['incoming']++;

            if ($code === '304') {
                $stats['missedIncoming']++;
                $stats['employees'][$name]['missed_incoming']++;
            }
        } else { // OUTGOING
            $stats['totalOutgoing']++;
            $stats['employees'][$name]['outgoing']++;

            // считаем пропущенным исходящий, если код задан и не равен 200
            if ($code !== '' && $code !== '200') {
                $stats['missedOutgoing']++;
                $stats['employees'][$name]['missed_outgoing']++;
            }
        }
    }

    $logger->log('INCOMING', 'Простая статистика вычислена для ' . count($stats['employees']) . ' сотрудников');
    return $stats;
}

// Формирование сообщения
function buildMessage(array $stats, DateTime $fromLocal): string {
    $date = $fromLocal->format('Y-m-d');

    $lines = [
        "Статистика звонков за {$date}",
        "Всего звонков: {$stats['totalCalls']}",
        "Входящих: {$stats['totalIncoming']}",
        "Исходящих: {$stats['totalOutgoing']}",
        "Пропущенных входящих: {$stats['missedIncoming']}",
        "Пропущенных исходящих: {$stats['missedOutgoing']}",
        "",
        "По менеджерам:",
    ];

    foreach ($stats['employees'] as $name => $st) {
        $lines[] = " {$name}";
        $lines[] = "— Всего: {$st['total_calls']}";
        $lines[] = "— Входящих: {$st['incoming']}";
        $lines[] = "— Исходящих: {$st['outgoing']}";
        $lines[] = "— Пропущенных входящих: {$st['missed_incoming']}";
        $lines[] = "— Пропущенных исходящих: {$st['missed_outgoing']}";
    }

    return implode("\n", $lines);
}


// Отправка сообщения в чат
function sendMessage(string $message, int $dialogId, Logger $logger): void {
    $params = ['DIALOG_ID' => (string)$dialogId, 'MESSAGE' => $message, 'SYSTEM' => 'Y'];
    $logger->log('OUTGOING', "Отправка сообщения в чат {$dialogId}");
    $resp = CRest::call('im.message.add', $params);
    if (isset($resp['error'])) {
        $logger->log('OUTGOING', 'Ошибка при отправке: ' . $resp['error_description']);
        throw new RuntimeException('Error sending message: ' . $resp['error_description']);
    }
    $logger->log('INCOMING', 'Сообщение успешно отправлено в чат ' . $dialogId);
}

// Основная логика одного запуска
function runOnce(array $dialogIds, Logger $logger): void {
    $fromLocal = new DateTime('today', new DateTimeZone('Europe/Moscow'));
    $toLocal   = new DateTime('now',   new DateTimeZone('Europe/Moscow'));
    $fromDate  = $fromLocal->format('Y-m-d\\TH:i:s');
    $toDate    = $toLocal->format('Y-m-d\\TH:i:s');

    $logger->log('OUTGOING', "Диапазон дат: от {$fromDate} до {$toDate}");
    $rawCalls      = fetchRawCalls($fromDate, $toDate, $logger);
    $managerMap    = fetchManagers($logger);
    $filteredCalls = filterCalls($rawCalls, $managerMap, $logger);
    $stats         = computeStatistics($filteredCalls, $logger);
    $message       = buildMessage($stats, $fromLocal);

    foreach ($dialogIds as $did) {
        sendMessage($message, $did, $logger);
    }
}

// Вычисление следующего времени запуска (ежедневно в 13:00 и 17:30)
function getNextRun(): DateTime {
    $tz  = new DateTimeZone('Europe/Moscow');
    $now = new DateTime('now', $tz);
    $first  = new DateTime('today 07:00', $tz);
    $second = new DateTime('today 11:30', $tz);

    if ($now < $first) {
        return $first;
    }
    if ($now < $second) {
        return $second;
    }

    $first->modify('+1 day');
    return $first;
}

// === Точка входа ===
$logger = new Logger(__DIR__ . '/logs');
// ID чатов для отправки отчёта
$dialogIds = [80997];

$logger->log('OUTGOING', 'Демон запущен, ожидаем первого запуска для ' . implode(', ', $dialogIds));
while (true) {
    $nextRun = getNextRun();
    $now = new DateTime('now', new DateTimeZone('Europe/Moscow'));
    $sleep = $nextRun->getTimestamp() - $now->getTimestamp();
    if ($sleep > 0) {
        $logger->log('OUTGOING', 'Спим до ' . $nextRun->format('Y-m-d H:i:s'));
        sleep($sleep);
    }
    try {
        $logger->log('OUTGOING', 'Начало выполнения запланированной задачи');
        runOnce($dialogIds, $logger);
    } catch (Throwable $e) {
        $logger->log('OUTGOING', 'Ошибка во время выполнения: ' . $e->getMessage());
    }
}
