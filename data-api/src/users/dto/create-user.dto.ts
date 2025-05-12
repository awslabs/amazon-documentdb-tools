import { ApiProperty } from "@nestjs/swagger"

export class CreateUserDto {
  @ApiProperty({
    name: "username",
    type: "string",
    required: true,
    description: "The username of the user.",
    example: "read-api-key",
  })
  username: string

  @ApiProperty({
    name: "type",
    type: "string",
    required: true,
    description: "The type of user being created.",
    example: "api-key",
  })
  type: "api-key"

  @ApiProperty({
    name: "role",
    type: "string",
    required: true,
    description:
      "The role of the user being created. Available roles are 'admin', 'read', 'write' or 'readWrite'.",
    example: "admin",
  })
  role: "admin" | "read" | "write" | "readWrite"

  @ApiProperty({
    name: "key",
    type: "string",
    required: true,
    description: "The api key to be used for the user being created.",
    example: "4071b0d2fbbdaf6620ed3a0ce45e37af596ce9f1b93d55b4e556682198c08cf9",
  })
  key: string

  @ApiProperty({
    name: "expires_at",
    type: "string",
    format: "date-time",
    required: true,
    description:
      "The role of the user being created. Available roles are 'admin', 'read', 'write' or 'readWrite'.",
    example: "2024-12-31T23:59:59.999Z",
  })
  expires_at: Date
}
