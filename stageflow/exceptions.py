from __future__ import annotations


class StageFlowError(Exception):
    def __str__(self) -> str:
        if len(self.args) == 1 and isinstance(self.args[0], str):
            return self.args[0]
        return Exception.__str__(self)


class RegistryError(StageFlowError, ValueError):
    pass


class PipelineDefinitionError(StageFlowError, ValueError):
    pass


class PipelineValidationError(StageFlowError, ValueError):
    def __init__(self, errors: list[str]):
        self.errors = list(errors)
        super().__init__("Pipeline validation failed: " + "; ".join(errors))


class ExpressionError(StageFlowError, RuntimeError):
    pass


class PayloadValidationError(StageFlowError, ValueError):
    pass


class StageContractError(StageFlowError, ValueError):
    pass


class StageOutputError(StageFlowError, KeyError):
    pass


class ArtifactNotFoundError(StageFlowError, KeyError):
    pass


class BranchError(StageFlowError, RuntimeError):
    pass


class TypeDeclarationError(StageFlowError, ValueError):
    pass


class TypeCheckError(StageFlowError, TypeError):
    pass
