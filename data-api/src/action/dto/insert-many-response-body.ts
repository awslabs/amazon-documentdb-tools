import { ApiProperty } from "@nestjs/swagger"

export class InsertManyResponseBody {
  @ApiProperty({
    name: "insertedCount",
    type: "number",
    required: true,
    description: "The number of documents inserted.",
    example: 2,
  })
  insertedCount: number

  @ApiProperty({
    name: "insertedIds",
    type: "array",
    items: { type: "string" },
    required: true,
    description: "A list of the _id values of the inserted documents.",
    example: ["6673345f8a3c6a6801571969", "667339078a3c6a680157196a"],
  })
  insertedIds: string[]
}
