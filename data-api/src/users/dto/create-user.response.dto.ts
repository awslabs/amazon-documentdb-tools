import { CreateUserDto } from "./create-user.dto"
import { ApiProperty } from "@nestjs/swagger"

export class CreateUserResponseDto extends CreateUserDto {
  @ApiProperty({
    name: "id",
    type: "string",
    description: "The unique identifier of the user.",
    example: "66b7eafce1aa52352516485a",
  })
  id: string

  @ApiProperty({
    name: "masked_key",
    type: "string",
    description: "The api key in masked format .",
    example: "66b7eafce1aa52352516485a",
  })
  masked_key: string

  @ApiProperty({
    name: "created_at",
    type: "string",
    format: "date-time",
    description: "The date and time the user was created.",
    example: "2024-08-10T22:22:18.672",
  })
  created_at: Date
}
