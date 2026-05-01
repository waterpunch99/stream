from collections import defaultdict
from datetime import datetime


events = [
    {"event_id": "e1", "event_time": "10:00:05", "arrival_time": "10:00:06", "product_id": "A"},
    {"event_id": "e2", "event_time": "10:00:20", "arrival_time": "10:00:21", "product_id": "A"},
    {"event_id": "e3", "event_time": "10:00:50", "arrival_time": "10:00:51", "product_id": "A"},
    {"event_id": "e4", "event_time": "10:00:10", "arrival_time": "10:01:05", "product_id": "A"},
]


def parse_time(time_str):
    return datetime.strptime(time_str, "%H:%M:%S")


def get_minute_window(time_str):
    
    time = parse_time(time_str)
    window_start = time.replace(second=0)
    window_end = window_start.replace(minute=window_start.minute + 1)

    return f"{window_start.strftime('%H:%M')}~{window_end.strftime('%H:%M')}"


def count_by_window(events, time_field):
    result = defaultdict(int)

    for event in events:
        window = get_minute_window(event[time_field])
        result[window] += 1

    return dict(result)


def print_events(events):
    print("이벤트 목록")
    for event in events:
        print(
            f"{event['event_id']} | "
            f"event_time={event['event_time']} | "
            f"arrival_time={event['arrival_time']} | "
            f"product_id={event['product_id']}"
        )
    print()


def print_result(title, result):
    print(f"=== {title} ===")
    for window, count in sorted(result.items()):
        print(f"{window}: {count}")
    print()


def main():
    print_events(events)

    arrival_time_result = count_by_window(events, "arrival_time")
    event_time_result = count_by_window(events, "event_time")

    print_result("arrival_time 기준 1분 window 집계", arrival_time_result)
    print_result("event_time 기준 1분 window 집계", event_time_result)

    print("관찰 포인트")
    print("arrival_time 기준에서는 e4가 10:01~10:02 window에 들어간다.")
    print("event_time 기준에서는 e4가 10:00~10:01 window에 들어간다.")
    print("따라서 10:00~10:01 window의 조회 수는 arrival_time 기준 3, event_time 기준 4가 된다.")


if __name__ == "__main__":
    main()