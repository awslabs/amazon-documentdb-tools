import { BaseRequestBody } from "./base-request-body"
import { ApiProperty, ApiPropertyOptional } from "@nestjs/swagger"

export class FindOneRequestBody extends BaseRequestBody {
  @ApiProperty({
    name: "filter",
    type: "object",
    required: true,
    description:
      "A MongoDB query filter that matches documents. For a list of all query operators that the Data API supports, see Query Operators.",
    example: { task: "Make a cup of coffee", status: "completed" },
  })
  filter: Record<string, any>

  @ApiPropertyOptional({
    name: "projection",
    type: "object",
    additionalProperties: {
      type: "number",
      enum: [0, 1],
    },
    required: false,
    description:
      "A MongoDB projection for matched documents returned by the operation.",
    example: { task: 1, status: 1, created_by: 1 },
  })
  projection: Record<string, any> | undefined
}
