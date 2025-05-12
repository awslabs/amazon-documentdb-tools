import { ApiProperty } from "@nestjs/swagger"

export class FindResponseBody {
  @ApiProperty({
    name: "documents",
    description: "A list of documents that match the specified filter.",
    nullable: true,
    type: "array",
    items: { type: "object" },
    example: [
      {
        _id: "6673345f8a3c6a6801571969",
        task: "Make a cup of coffee",
        created_by: "me",
      },
      {
        _id: "667339078a3c6a680157196a",
        task: "Learn Data-API",
        created_by: "me",
      },
    ],
  })
  documents: Record<string, any>[] | null
}
