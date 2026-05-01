records = [
    {
        "topic": "order-events",
        "partition": 2,
        "offset": 150,
        "key": "o-77",
        "value": {
            "event_id": "evt-1001",
            "event_type": "OrderPaid",
            "order_id": "o-77",
            "amount": 39000,
            "event_time": "10:00:00"
        }
    },
    {
        "topic": "order-events",
        "partition": 2,
        "offset": 151,
        "key": "o-77",
        "value": {
            "event_id": "evt-1001",
            "event_type": "OrderPaid",
            "order_id": "o-77",
            "amount": 39000,
            "event_time": "10:00:00"
        }
    }
]


def process_records_without_dedup(records):

    total_amount = 0

    print("record 단위 무조건 처리")

    for record in records:
        event = record["value"]

        print(
            f"처리 record: "
            f"partition={record['partition']}, "
            f"offset={record['offset']}, "
            f"event_id={event['event_id']}, "
            f"amount={event['amount']}"
        )

        total_amount += event["amount"]

    return total_amount


def process_records_with_event_id_dedup(records):
    
    total_amount = 0
    processed_event_ids = set()

    print("event_id 기준 중복 제거 처리")

    for record in records:
        event = record["value"]
        event_id = event["event_id"]

        if event_id in processed_event_ids:
            print(
                f"중복 skip: "
                f"partition={record['partition']}, "
                f"offset={record['offset']}, "
                f"event_id={event_id}"
            )
            continue

        print(
            f"처리 event: "
            f"partition={record['partition']}, "
            f"offset={record['offset']}, "
            f"event_id={event_id}, "
            f"amount={event['amount']}"
        )

        total_amount += event["amount"]
        processed_event_ids.add(event_id)

    return total_amount


def print_record_summary(records):
    print("입력 records")

    for record in records:
        event = record["value"]

        print(
            f"topic={record['topic']}, "
            f"partition={record['partition']}, "
            f"offset={record['offset']}, "
            f"key={record['key']}, "
            f"event_id={event['event_id']}, "
            f"order_id={event['order_id']}, "
            f"amount={event['amount']}"
        )

    print()


def main():
    print_record_summary(records)

    total_without_dedup = process_records_without_dedup(records)
    print(f"결과: {total_without_dedup}원")
    print()

    total_with_dedup = process_records_with_event_id_dedup(records)
    print(f"결과: {total_with_dedup}원")
    print()

    print("관찰 포인트")
    print("offset은 150, 151로 서로 다르다.")
    print("하지만 event_id는 evt-1001로 동일하다.")
    print("record는 2개지만 비즈니스 이벤트는 1개다.")
    print("중복 제거 없이 record마다 처리하면 결제 금액이 78000원으로 부풀어 오른다.")
    print("event_id 기준으로 중복 제거하면 실제 결제 금액인 39000원으로 유지된다.")


if __name__ == "__main__":
    main()