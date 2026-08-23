from repositories.knowledge_repository import KnowledgeRepository


class KnowledgeEngine:

    def __init__(self, repository: KnowledgeRepository):
        self.repository = repository

    def enabled_records(self):
        return self.repository.enabled()

    def by_category(self, category: str):
        return self.repository.by_category(category)

    def by_service_stage(self, stage: str):
        return self.repository.by_service_stage(stage)

    def by_mileage(self, mileage: int):
        return self.repository.by_mileage(mileage)

    def by_fuel_type(self, fuel_type: str):
        return self.repository.by_fuel_type(fuel_type)

    def by_vehicle_type(self, vehicle_type: str):
        return self.repository.by_vehicle_type(vehicle_type)

    def get_record(self, record_id: str):
        return self.repository.get(record_id)

    def get_applicable_inspections(
        self,
        vehicle_type: str,
        fuel_type: str,
        mileage: int,
        service_stage: str,
    ):
        records = self.repository.enabled()

        vehicle_type = vehicle_type.upper()
        fuel_type = fuel_type.upper()
        service_stage = service_stage.upper()

        applicable = []

        for record in records:

            # Vehicle Type
            if (
                record.vehicle_type != "ALL"
                and record.vehicle_type != vehicle_type
            ):
                continue

            # Fuel Type
            if (
                record.fuel_type != "ALL"
                and record.fuel_type != fuel_type
            ):
                continue

            # Minimum Mileage
            if (
                record.min_mileage is not None
                and mileage < record.min_mileage
            ):
                continue

            # Maximum Mileage
            if (
                record.max_mileage is not None
                and mileage > record.max_mileage
            ):
                continue

            # Service Stage
            if record.service_stage.upper() != service_stage:
                continue

            applicable.append(record)

        return applicable