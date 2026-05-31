events = [
    {
        "event_id": "e3",
        "event_type": "OrderPaid",
        "user_id": "u-1",
        "event_time": "10:00:10",
        "arrival_time": "10:00:11",
        "offset": 100,
    },
    {
        "event_id": "e2",
        "event_type": "AddToCart",
        "user_id": "u-1",
        "event_time": "10:00:05",
        "arrival_time": "10:00:13",
        "offset": 101,
    },
    {
        "event_id": "e1",
        "event_type": "ProductViewed",
        "user_id": "u-1",
        "event_time": "10:00:00",
        "arrival_time": "10:00:12",
        "offset": 102,
    },
]


def to_seconds(t):
    h, m, s = map(int, t.split(":"))
    return h * 3600 + m * 60 + s


def print_events_by_order(events, order_field):
    print(f"{order_field} 기준 이벤트 순서")

    for event in sorted(events, key=lambda x: x[order_field]):
        print(
            f"offset={event['offset']} | "
            f"event_id={event['event_id']} | "
            f"type={event['event_type']} | "
            f"user={event['user_id']} | "
            f"event_time={event['event_time']} | "
            f"arrival_time={event['arrival_time']}"
        )

    print()


def detect_by_arrival_order(events):
    last_view_time_by_user = {}
    matched_users = set()

    print("arrival_time 기준 처리 로그")

    for event in sorted(events, key=lambda x: x["arrival_time"]):
        user_id = event["user_id"]
        event_type = event["event_type"]

        print(
            f"처리 중: {event['event_id']} "
            f"type={event_type}, "
            f"event_time={event['event_time']}, "
            f"arrival_time={event['arrival_time']}"
        )

        if event_type == "ProductViewed":
            last_view_time_by_user[user_id] = to_seconds(event["event_time"])
            print(f"상태 저장: user={user_id}, last_view_time={event['event_time']}")

        if event_type == "OrderPaid":
            view_time = last_view_time_by_user.get(user_id)

            if view_time is None:
                print(f"판정 실패: user={user_id}의 ProductViewed 상태가 아직 없음")
            else:
                paid_time = to_seconds(event["event_time"])
                diff = paid_time - view_time

                if 0 <= diff <= 10:
                    matched_users.add(user_id)
                    print(f"조건 만족: user={user_id}, diff={diff}초")
                else:
                    print(f"조건 불만족: user={user_id}, diff={diff}초")

        print(f"현재 상태: {last_view_time_by_user}")
        print()

    return matched_users


def detect_by_event_time_order(events):
    last_view_time_by_user = {}
    matched_users = set()

    print("event_time 기준 처리 로그")

    for event in sorted(events, key=lambda x: x["event_time"]):
        user_id = event["user_id"]
        event_type = event["event_type"]

        print(
            f"처리 중: {event['event_id']} "
            f"type={event_type}, "
            f"event_time={event['event_time']}, "
            f"arrival_time={event['arrival_time']}"
        )

        if event_type == "ProductViewed":
            last_view_time_by_user[user_id] = to_seconds(event["event_time"])
            print(f"상태 저장: user={user_id}, last_view_time={event['event_time']}")

        if event_type == "OrderPaid":
            view_time = last_view_time_by_user.get(user_id)

            if view_time is None:
                print(f"판정 실패: user={user_id}의 ProductViewed 상태가 아직 없음")
            else:
                paid_time = to_seconds(event["event_time"])
                diff = paid_time - view_time

                if 0 <= diff <= 10:
                    matched_users.add(user_id)
                    print(f"조건 만족: user={user_id}, diff={diff}초")
                else:
                    print(f"조건 불만족: user={user_id}, diff={diff}초")

        print(f"현재 상태: {last_view_time_by_user}")
        print()

    return matched_users


def main():
    print_events_by_order(events, "arrival_time")
    print_events_by_order(events, "event_time")
    print_events_by_order(events, "offset")

    matched_by_arrival = detect_by_arrival_order(events)
    print(f"arrival_time 기준 탐지 결과: {matched_by_arrival}")
    print()

    matched_by_event_time = detect_by_event_time_order(events)
    print(f"event_time 기준 탐지 결과: {matched_by_event_time}")
    print()

    print("관찰 포인트")
    print("도착 순서 기준으로는 OrderPaid가 ProductViewed보다 먼저 처리된다.")
    print("그래서 OrderPaid를 처리하는 시점에는 ProductViewed 상태가 아직 없다.")
    print("결과적으로 arrival_time 기준 탐지 결과는 빈 set이 된다.")
    print()
    print("event_time 기준으로는 ProductViewed가 먼저 처리되고 OrderPaid가 나중에 처리된다.")
    print("ProductViewed 10:00:00 이후 OrderPaid 10:00:10이므로 10초 이내 결제 조건을 만족한다.")
    print("결과적으로 event_time 기준 탐지 결과는 {'u-1'}이 된다.")
    print()
    print("하지만 실제 스트리밍에서는 전체 이벤트를 무한정 기다렸다가 정렬할 수 없다.")
    print("그래서 window, watermark, allowed lateness 같은 개념이 필요해진다.")


if __name__ == "__main__":
    main()