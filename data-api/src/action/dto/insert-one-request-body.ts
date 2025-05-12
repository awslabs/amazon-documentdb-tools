import { BaseRequestBody } from "./base-request-body"
import { ApiProperty } from "@nestjs/swagger"

export class InsertOneRequestBody extends BaseRequestBody {
  @ApiProperty({
    name: "document",
    type: "object",
    required: true,
    description: "A document to insert into the collection.",
    example: {
      task: "Make a cup of coffee",
      status: "completed",
      created_by: "me",
    },
  })
  document: Record<string, any>
}
