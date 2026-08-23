import yaml
from pathlib import Path

from repositories.knowledge_repository import (
    KnowledgeRecord,
    KnowledgeRepository,
)



class KnowledgeLoader:

    def __init__(self):

        self.repository = KnowledgeRepository()

        self.knowledge_path = (
            Path(__file__).parent.parent
            / "knowledge"
            / "universal"
        )

    def find_markdown_files(self):

        return sorted(
            self.knowledge_path.rglob("*.md")
        )

    
    def all_records(self):
        return self.records
    
    def print_files(self):

        files = self.find_markdown_files()

        print(f"Found {len(files)} knowledge files")

        for file in files:
            print(file)

    def read_file(self, file_path: Path) -> str:

        with open(file_path, "r", encoding="utf-8") as file:
            return file.read()

    def parse_markdown(self, file_path: Path) -> KnowledgeRecord:

        content = self.read_file(file_path)

        if not content.startswith("---"):
            raise ValueError(f"{file_path} has no YAML front matter.")

        parts = content.split("---", 2)

        yaml_text = parts[1]

        notes = parts[2].strip()

        metadata = yaml.safe_load(yaml_text)

        return KnowledgeRecord(
            id=metadata["id"],
            category=metadata["category"],
            subcategory=metadata["subcategory"],
            title=metadata["title"],
            priority=metadata["priority"],

            vehicle_type=metadata["vehicle_type"],
            fuel_type=metadata["fuel_type"],

            year_from=metadata.get("year_from"),
            year_to=metadata.get("year_to"),

            min_mileage=metadata.get("min_mileage"),
            max_mileage=metadata.get("max_mileage"),

            time_interval_months=metadata.get("time_interval_months"),

            service_stage=metadata["service_stage"],

            inspection_reason=metadata["inspection_reason"].strip(),
            recommendation=metadata["recommendation"].strip(),

            source=metadata.get("source", []),

            enabled=metadata.get("enabled", True),

            notes=notes,

            file_path=file_path,
        )

    def load_all(self) -> KnowledgeRepository:

        self.repository = KnowledgeRepository()

        files = self.find_markdown_files()

        for file in files:
            record = self.parse_markdown(file)
            self.repository.add(record)

        return self.repository

    def test_read(self):

        files = self.find_markdown_files()

        if not files:
            print("No knowledge files found.")
            return

        first_file = files[0]

        print(f"Reading: {first_file}")

        record = self.parse_markdown(first_file)

        print(record)