import { ApiProperty } from "@nestjs/swagger"

export class FindOneResponseBody {
  @ApiProperty({
    name: "document",
    description:
      "A document that matches the specified filter. If no documents match, this is null",
    oneOf: [
      {
        type: "object",
        example: {
          _id: "667348518a3c6a680157196b",
          task: "Make a cup of coffee",
          created_by: "me",
        },
      },
      {
        type: "null",
        example: null,
      },
    ],
  })
  document: Record<string, any> | null
}
