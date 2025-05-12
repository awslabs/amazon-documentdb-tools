import { ApiProperty } from '@nestjs/swagger';

export class ErrorResponse {
  @ApiProperty({
    name: "error",
    description: "A message that describes the error.",
    type: "string",
    example: "The request body is missing a required field.",
  })
  error: string;

  @ApiProperty({
    name: "error_code",
    description: "The error type.",
    type: "string",
    example: "InvalidRequest",
  })
  error_code: string;

  @ApiProperty({
    name: "link",
    description: "A link to a log entry for the failed operation.",
    type: "string",
  })
  link: string
}
