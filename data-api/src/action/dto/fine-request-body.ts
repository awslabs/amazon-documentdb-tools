import { FindOneRequestBody } from "./fine-one-request-body"
import { ApiProperty, ApiPropertyOptional } from "@nestjs/swagger"
import { SortDirection } from "mongodb"
import { IsInt, IsOptional } from "class-validator"

export class FindRequestBody extends FindOneRequestBody {
  @ApiProperty({
    name: "filter",
    type: "object",
    required: true,
    description:
      "A MongoDB query filter that matches documents. For a list of all query operators that the Data API supports, see Query Operators.",
    example: { status: "pending" },
  })
  filter: Record<string, any>

  @ApiPropertyOptional({
    name: "sort",
    type: "object",
    additionalProperties: {
      type: "number",
      enum: [-1, 1],
    },
    required: false,
    description:
      "A MongoDB sort expression that indicates sorted field names and directions.",
    example: { task: 1 },
  })
  sort: Record<string, SortDirection> | undefined

  @ApiPropertyOptional({
    name: "limit",
    type: "number",
    required: false,
    description:
      "The maximum number of matching documents to include the in the response.",
    example: 10,
  })
  limit: number | undefined

  @ApiPropertyOptional({
    name: "skip",
    type: "number",
    required: false,
    description: "The number of matching documents to omit from the response.",
    example: 1,
  })
  skip: number | undefined
}
