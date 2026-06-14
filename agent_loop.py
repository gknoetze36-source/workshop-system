class AgentLoop:
    def __init__(self, workspace):
        self.workspace = workspace

    def plan(self, task):
        return f"Planning: {task}"

    def execute(self, plan):
        return f"Executing safely: {plan}"

    def validate(self, result):
        return "PASS"

    def run(self, task):
        plan = self.plan(task)
        result = self.execute(plan)

        if self.validate(result) == "PASS":
            return {
                "status": "success",
                "plan": plan,
                "result": result
            }

        return {
            "status": "blocked",
            "reason": "validation failed"
        }