import { BaseRequestBody } from "./base-request-body"
import { ApiProperty } from "@nestjs/swagger"

export class InsertManyRequestBody extends BaseRequestBody {
  @ApiProperty({
    name: "documents",
    type: "array",
    items: { type: "object" },
    required: true,
    description: "A list of documents to insert into the collection.",
    example: [
      { task: "Learn Data-API", status: "pending", created_by: "me" },
      { task: "Give Data-API demo", status: "pending", created_by: "me" },
      { task: "Enhance features", status: "pending", created_by: "others" },
    ],
  })
  documents: [Record<string, any>]
}
