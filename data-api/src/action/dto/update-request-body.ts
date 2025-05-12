import { BaseRequestBody } from "./base-request-body"
import { ApiProperty, ApiPropertyOptional } from "@nestjs/swagger"

export class UpdateRequestBody extends BaseRequestBody {
  @ApiProperty({
    name: "filter",
    type: "object",
    required: true,
    description:
      "A MongoDB query filter that matches documents. For a list of all query operators that the Data " +
      "API supports, see Query Operators.",
    example: {
      created_by: "others",
    },
  })
  filter: [Record<string, any>]

  @ApiProperty({
    name: "update",
    type: "object",
    required: true,
    description:
      "A MongoDB update expression to apply to matching documents. For a list of all update operators " +
      "that the Data API supports, see Update Operators.",
    example: {
      $set: {
        status: "started",
      },
    },
  })
  update: Record<string, any>

  @ApiPropertyOptional({
    name: "upsert",
    type: "boolean",
    required: false,
    description:
      "When true, if the update filter does not match any existing documents, then insert a new document " +
      "based on the filter and the specified update operation.",
    example: false,
  })
  upsert: boolean = false
}
