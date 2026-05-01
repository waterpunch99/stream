events = [
    {
        "event_id": "e1",
        "event_time": "10:00:05",
        "ingestion_time": "10:00:06",
        "processing_time": "10:00:07",
        "amount": 10000,
    },
    {
        "event_id": "e2",
        "event_time": "10:00:20",
        "ingestion_time": "10:00:22",
        "processing_time": "10:00:24",
        "amount": 20000,
    },
    {
        "event_id": "e3",
        "event_time": "10:00:50",
        "ingestion_time": "10:00:52",
        "processing_time": "10:01:10",
        "amount": 30000,
    },
    {
        "event_id": "e4",
        "event_time": "10:00:10",
        "ingestion_time": "10:01:05",
        "processing_time": "10:01:06",
        "amount": 40000,
    },
]


def to_seconds(time_str):
    h, m, s = map(int, time_str.split(":"))
    return h * 3600 + m * 60 + s


def to_time_string(seconds):
    h = seconds // 3600
    remaining = seconds % 3600
    m = remaining // 60
    s = remaining % 60

    return f"{h:02d}:{m:02d}:{s:02d}"


def window_start(time_str, window_size_seconds=60):
    seconds = to_seconds(time_str)
    return seconds - (seconds % window_size_seconds)


def window_label(window_start_seconds, window_size_seconds=60):
    start = window_start_seconds
    end = window_start_seconds + window_size_seconds

    return f"{to_time_string(start)}~{to_time_string(end)}"


def aggregate_by_time_field(events, time_field):
    result = {}

    for event in events:
        ws = window_start(event[time_field])
        result[ws] = result.get(ws, 0) + event["amount"]

    return result


def print_events(events):
    print("입력 이벤트")

    for event in events:
        print(
            f"{event['event_id']} | "
            f"event_time={event['event_time']} | "
            f"ingestion_time={event['ingestion_time']} | "
            f"processing_time={event['processing_time']} | "
            f"amount={event['amount']}"
        )

    print()


def print_aggregate_result(time_field, result):
    print(f"{time_field} 기준 1분 매출 집계")

    for ws in sorted(result.keys()):
        print(f"{window_label(ws)}: {result[ws]}원")

    print()


def print_event_window_mapping(events):
    print("이벤트별 window 매핑 비교")

    for event in events:
        event_window = window_label(window_start(event["event_time"]))
        ingestion_window = window_label(window_start(event["ingestion_time"]))
        processing_window = window_label(window_start(event["processing_time"]))

        print(
            f"{event['event_id']} | "
            f"event_time 기준={event_window} | "
            f"ingestion_time 기준={ingestion_window} | "
            f"processing_time 기준={processing_window}"
        )

    print()


def main():
    print_events(events)
    print_event_window_mapping(events)

    for field in ["event_time", "ingestion_time", "processing_time"]:
        result = aggregate_by_time_field(events, field)
        print_aggregate_result(field, result)

    print("관찰 포인트")
    print("event_time 기준으로는 모든 이벤트가 10:00~10:01 window에 들어간다.")
    print("ingestion_time 기준으로는 e4가 10:01~10:02 window에 들어간다.")
    print("processing_time 기준으로는 e3와 e4가 10:01~10:02 window에 들어간다.")
    print("따라서 같은 이벤트 목록이라도 시간 기준에 따라 매출 집계 결과가 달라진다.")


if __name__ == "__main__":
    main()