export enum ErrorMessages {
  INVALID_REQUEST = "The request was malformed or incomplete.",
  UNAUTHORIZED_REQUEST = "The request was missing a valid authentication token.",
  FORBIDDEN_REQUEST = "The authenticated user does not have permission to access this endpoint."
}