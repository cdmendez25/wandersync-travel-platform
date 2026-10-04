from graphql import GraphQLError


class AppError(GraphQLError):
    """Error that is safe to show to the client. Anything else is masked."""

    def __init__(self, message: str, code: str, **extra):
        super().__init__(message, extensions={"code": code, **extra})


def should_mask_error(error: GraphQLError) -> bool:
    # Validation errors have no original error; AppErrors are intentional.
    # Everything else (bugs, DB errors, stack details) becomes "Unexpected error."
    return error.original_error is not None and not isinstance(error.original_error, GraphQLError)
