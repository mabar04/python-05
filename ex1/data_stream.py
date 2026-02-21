from abc import ABC, abstractmethod
from typing import List, Any, Optional, Dict, Union


class DataStream(ABC):
    def __init__(self, stream_id: str) -> None:
        self.stream_id = stream_id

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(self, data_batch: List[Any], criteria: Optional[str]
                    = None) -> List[Any]:
        if criteria:
            new_Data = []
            for data in data_batch:
                if data == criteria:
                    new_Data.append(data)
            return new_Data
        else:
            return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "Stream ID": "Datastream_001",
            "Type": "Overll Data"
        }


class SensorStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.count = 0

    def process_batch(self, data_batch: List[str]) -> str:
        total_tamp = 0
        j = 0
        for data in data_batch:
            str1, str2 = data.split(":")
            try:
                if str1 == "temp":
                    total_tamp += float(str2)
                    j += 1
            except ValueError:
                continue
            self.count += 1
        try:
            return (f"Sensor analysis: {self.count} reading processed, "
                    f"avg temp: "
                    f"{total_tamp / j}°C")
        except ZeroDivisionError:
            return (f"Sensor analysis: {self.count} reading processed, "
                    f"avg temp: 0°C")

    def filter_data(self, data_batch: List[str], criteria: Optional[str]
                    = None) -> List[Any]:
        if criteria:
            filtered_data = []
            for data in data_batch:
                str1, str2 = data.split(":")
                if str1 == criteria:
                    filtered_data.append(data)
            return filtered_data
        else:
            return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "Stream ID": self.stream_id,
            "Type": "Environmental Data",
            "Sensor data": f"{self.count} readings processed"
        }


class TransactionStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.count = 0

    def process_batch(self, data_batch: List[str]) -> str:
        total_gain = 0
        for data in data_batch:
            str1, str2 = data.split(":")
            self.count += 1
            if str1 == "buy":
                try:
                    total_gain += int(str2)
                except ValueError:
                    continue
            elif str1 == "sell":
                try:
                    total_gain -= int(str2)
                except ValueError:
                    continue
        if total_gain > 0:
            return (f"Transaction analysis: {self.count} operations,"
                    f" net flow: +{total_gain} units")
        else:
            return (f"Transaction analysis: {self.count} operations,"
                    f" net flow: {total_gain} units")

    def filter_data(self, data_batch: List[str], criteria: Optional[str]
                    = None) -> List[Any]:
        if criteria:
            filtered_data = []
            for data in data_batch:
                str1, str2 = data.split(":")
                if float(str2) >= float(criteria):
                    filtered_data.append(data)
            return filtered_data
        else:
            return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "Stream ID": self.stream_id,
            "Type": "Financial Data",
            "Transaction data": f"{self.count} operations processed"
        }


class EventStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)
        self.count = 0

    def process_batch(self, data_batch: List[str]) -> str:
        errors = 0
        self.count = 0
        for data in data_batch:
            if data == "error":
                errors += 1
            self.count += 1
        return (f"Event analysis: {self.count} events,"
                f" {errors} error detected")

    def filter_data(self, data_batch: List[str], criteria: Optional[str]
                    = None) -> List[Any]:
        if criteria:
            filtered_data = []
            for data in data_batch:
                str1, str2 = data.split(":")
                if str1 == criteria:
                    filtered_data.append(data)
            return filtered_data
        else:
            return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "Stream ID": self.stream_id,
            "Type": "System Events",
            "Event data": f"{self.count} events processed"
        }


class StreamProcessor:
    def stream_handle(self, stream: DataStream, data: List[str]) -> None:
        stream.process_batch(data)
        for key, value in stream.get_stats().items():
            if key != "Stream ID" and key != "Type":
                print(f"- {key}: {value}")


def main() -> None:
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===\n")
    print("Initializing Sensor Stream...")
    sensor_01 = SensorStream("SENSOR_001")
    for key, value in sensor_01.get_stats().items():
        print(f"{key} : {value}", end="")
        if key != "Type":
            print(",", end=" ")
        if key == "Type":
            break
    print()
    processing_data = ["temp:22.5", "humidity:65", "pressure:1013"]
    print(f"Processing sensor batch: {processing_data}")
    print(sensor_01.process_batch(processing_data))
    print()
    print("Initializing Transaction Stream...")
    trans_01 = TransactionStream("TRANS_001")
    for key, value in trans_01.get_stats().items():
        print(f"{key} : {value}", end="")
        if key != "Type":
            print(",", end=" ")
        if key == "Type":
            break
    print()
    transaction_data = ["buy:100", "sell:150", "buy:75"]
    print(f"Processing transaction batch: {transaction_data}")
    print(trans_01.process_batch(transaction_data), end="\n\n")
    print("Initializing Event Stream...")
    event_01 = EventStream("EVENT_001")
    for key, value in event_01.get_stats().items():
        print(f"{key} : {value}", end="")
        if key != "Type":
            print(",", end=" ")
        if key == "Type":
            break
    print()
    event_data = ["login", "error", "logout"]
    print(f"Processing event batch: {event_data}")
    print(event_01.process_batch(event_data), end="\n\n")
    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...\n")
    print("Batch 1 Results:")
    data_stream = [
        ["temp:22.5", "humidity:65"],
        ["buy:100", "sell:150", "buy:75", "buy:80"],
        ["login", "error", "logout"]
    ]
    sensor_02 = SensorStream("SENSOR_002")
    trans_02 = TransactionStream("TRANS_002")
    event_02 = EventStream("EVENT_002")
    streams = [sensor_02, trans_02, event_02]
    stream_processor = StreamProcessor()
    for i in range(len(data_stream)):
        stream_processor.stream_handle(streams[i], data_stream[i])
    print()
    print("Stream filtering active: High-priority data only")
    datastream_filter = ["temp:22", "temp:45", "alert:overheat",
                         "alert:Underheat"]
    datatrans_filter = ["buy:100", "sell:150", "buy:75", "buy:1000"]
    print(f'Filtered results: '
          f'{len(sensor_01.filter_data(datastream_filter, "alert"))}'
          f' critical sensor alerts, '
          f'{len(trans_01.filter_data(datatrans_filter, "1000"))} '
          f'large transaction')
    print()
    print("All streams processed successfully. Nexus throughput optimal.")


if __name__ == "__main__":
    main()
