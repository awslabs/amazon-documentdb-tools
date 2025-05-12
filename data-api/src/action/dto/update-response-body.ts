import { ApiProperty, ApiPropertyOptional } from "@nestjs/swagger"

export class UpdateResponseBody {
  @ApiProperty({
    name: "matchedCount",
    type: "number",
    required: true,
    description: "The number of documents matched by the query filter.",
    example: 1,
  })
  matchedCount: number

  @ApiProperty({
    name: "modifiedCount",
    type: "number",
    required: true,
    description: "The number of matched documents that were modified.",
    example: 1,
  })
  modifiedCount: number

  @ApiPropertyOptional({
    name: "upsertedCount",
    type: "number",
    required: false,
    description: "The number of documents that were inserted.",
    example: 1,
  })
  upsertedCount: number

  @ApiProperty({
    name: "upsertedId",
    type: "string",
    required: false,
    description: "The _id value of the upserted document.",
    example: "6673345f8a3c6a6801571969",
  })
  upsertedId: string
}
