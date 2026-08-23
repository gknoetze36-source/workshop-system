from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class KnowledgeRecord:
    id: str
    category: str
    subcategory: str
    title: str
    priority: str

    vehicle_type: str
    fuel_type: str

    year_from: Optional[int]
    year_to: Optional[int]

    min_mileage: Optional[int]
    max_mileage: Optional[int]

    time_interval_months: Optional[int]

    service_stage: str

    inspection_reason: str
    recommendation: str

    source: list[str]

    enabled: bool

    notes: str

    file_path: Path


class KnowledgeRepository:

    def __init__(self):
        self.records: list[KnowledgeRecord] = []

    def add(self, record: KnowledgeRecord):
        self.records.append(record)

    def all_records(self):
        return self.records

    def get(self, record_id: str):

        for record in self.records:
            if record.id == record_id:
                return record

        return None

    def by_category(self, category: str):

        return [
            record
            for record in self.records
            if record.category == category
        ]

    def by_service_stage(self, stage: str):

        return [
            record
            for record in self.records
            if record.service_stage == stage
        ]

    def enabled(self):

        return [
            record
            for record in self.records
            if record.enabled
        ]

    def by_mileage(self, mileage: int):

        applicable = []

        for record in self.records:

            if record.min_mileage is not None:
                if mileage < record.min_mileage:
                    continue

            if record.max_mileage is not None:
                if mileage > record.max_mileage:
                    continue

            applicable.append(record)

        return applicable

    def by_fuel_type(self, fuel_type: str):

        fuel_type = fuel_type.upper()

        return [
            record
            for record in self.records
            if record.fuel_type == "ALL"
            or record.fuel_type == fuel_type
        ]

    def by_vehicle_type(self, vehicle_type: str):

        vehicle_type = vehicle_type.upper()

        return [
            record
            for record in self.records
            if record.vehicle_type == "ALL"
            or record.vehicle_type == vehicle_type
        ]