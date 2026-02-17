from typing import Any, List
from abc import ABC, abstractmethod


class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


class NumericProcessor(DataProcessor):
    def process(self, data: List[int | float]) -> str:
        count = len(data)
        sum_data = sum(data)
        avg_data = sum_data / count
        return (f"Processed {count} numeric values, "
                f"sum={sum_data}, avg={avg_data}")

    def validate(self, data: list[int | float]) -> bool:
        if not isinstance(data, list):
            return False
        if len(data) == 0:
            return False
        for item in data:
            if not isinstance(item, int) and not isinstance(item, float):
                return False
        return True


class TextProcessor(DataProcessor):
    def process(self, data: str) -> str:
        word_count = len(data.split())
        return (f"Processed text: {len(data)} characters, "
                f"{word_count} words")

    def validate(self, data: str) -> bool:
        if not isinstance(data, str):
            return False
        return True


class LogProcessor(DataProcessor):
    def process(self, data: str) -> str:
        str1, str2 = data.split(":")
        str2.trim()
        if str1 == "ERROR":
            r = "ALERT"
        else:
            r = "INFO"
        return (f"[{r}] {str1} level detected: {str2}")

    def validate(self, data: str) -> bool:
        if not isinstance(data, str):
            return False
        if ":" not in data:
            return False
        return True


def main():
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")
    list_proc = [1, 2, 3, 4, 5]
    print("Initializing Numeric Processor...")
    print(f"Processing data: {list_proc}")
    numeric = NumericProcessor()
    if numeric.validate(list_proc):
        print("Validation: Numeric data verified")
        print(numeric.format_output(numeric.process(list_proc)))
    else:
        print("Validation: Numeric data not verified")
    print()
    print("Initializing Text Processor...")
    print('"Processing data: "Hello Nexus World"')
    text_hello = TextProcessor()
    text_test = "Hello Nexus World"
    if text_hello.validate("Hello Nexus World"):
        print("Validation: Text data verified")
        print(text_hello.format_output(text_hello.process(text_test)))
    else:
        print("Validation: Text data not verified")
    print()
    print("Initializing Log Processor...")
    test_log = "ERROR: Connection timeout"
    print(f'Processing data: "{test_log}"')
    log_inst = LogProcessor()
    if log_inst.validate(test_log):
        print("Validation: Log entry verified")
        print(log_inst.format_output(log_inst.process(test_log)))
    else:
        print("Validation: Log entry not verified")
    print()
    print("=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")
    print(f"Result 1: {numeric.process([1, 2, 3])}")
    print(f'Result 2: {text_hello.process("Hello Nexus ")}')
    print(f'Result 3: {log_inst.process("INFO: System ready")}')
    print("\nFoundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    main()
