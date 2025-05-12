import { BaseRequestBody } from "./base-request-body"
import { ApiProperty } from "@nestjs/swagger"

export class AggregateRequestBody extends BaseRequestBody {
  @ApiProperty({
    name: "pipeline",
    type: "array",
    items: { type: "object" },
    required: true,
    description: "An array of aggregation stages.",
    example: [
      { $group: { _id: "$status", count: { $sum: 1 } } },
      { $sort: { _id: 1 } },
    ],
  })
  pipeline: Record<string, any>[]
}
