import { BaseRequestBody } from "./base-request-body"
import { ApiProperty } from "@nestjs/swagger"

export class DeleteRequestBody extends BaseRequestBody {
  @ApiProperty({
    name: "filter",
    type: "object",
    required: true,
    description:
      "A MongoDB query filter that matches documents. For a list of all query operators that the Data API supports, see Query Operators.",
    example: { task: "Make a cup of coffee" },
  })
  filter: [Record<string, any>]
}
