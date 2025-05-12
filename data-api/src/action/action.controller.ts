import { Body, Controller, HttpCode, Post, UseGuards } from "@nestjs/common"
import { ActionService } from "./action.service"
import { UpdateRequestBody } from "./dto/update-request-body"
import { InsertOneRequestBody } from "./dto/insert-one-request-body"
import { InsertManyRequestBody } from "./dto/insert-many-request-body"
import { FindOneRequestBody } from "./dto/fine-one-request-body"
import { FindRequestBody } from "./dto/fine-request-body"
import { DeleteRequestBody } from "./dto/delete-request-body"
import { AggregateRequestBody } from "./dto/aggregate-request-body"
import {
  ApiBadRequestResponse,
  ApiCreatedResponse,
  ApiForbiddenResponse,
  ApiOkResponse,
  ApiOperation,
  ApiSecurity,
  ApiTags,
  ApiUnauthorizedResponse,
} from "@nestjs/swagger"
import { InsertOneResponseBody } from "./dto/insert-one-response-body"
import { InsertManyResponseBody } from "./dto/insert-many-response-body"
import { FindOneResponseBody } from "./dto/find-one-response-body"
import { ErrorResponse } from "./dto/error-response"
import { FindResponseBody } from "./dto/find-response-body"
import { AggregateResponseBody } from "./dto/aggregate-response-body"
import { ErrorMessages } from "../utils/messages"
import { DeleteResponseBody } from "./dto/delete-response-body"
import { UpdateResponseBody } from "./dto/update-response-body"
import { ApiKeyGuard } from "../middleware/api-key.guard"
import { Roles, UserRole, UserRoleGuard } from "../middleware/user-role.guard"

@ApiTags("Data API")
@UseGuards(ApiKeyGuard, UserRoleGuard)
@Controller("/action")
export class ActionController {
  constructor(private readonly actionService: ActionService) {}

  @Roles(UserRole.WriteOnly, UserRole.Admin, UserRole.ReadWrite)
  @ApiSecurity("apiKey")
  @Post("insertOne")
  @ApiOperation({
    summary: "Insert One Document",
    description: "Insert a single document into a collection.",
  })
  @ApiCreatedResponse({
    description: "The result of an insertOne operation.",
    type: InsertOneResponseBody,
  })
  @ApiBadRequestResponse({
    description: ErrorMessages.INVALID_REQUEST,
    type: ErrorResponse,
  })
  @ApiUnauthorizedResponse({
    description: ErrorMessages.UNAUTHORIZED_REQUEST,
    type: ErrorResponse,
  })
  @ApiForbiddenResponse({
    description: ErrorMessages.FORBIDDEN_REQUEST,
    type: ErrorResponse,
  })
  async insertOne(@Body() insertOneRequest: InsertOneRequestBody) {
    return await this.actionService.insertOne(insertOneRequest)
  }

  @Roles(UserRole.WriteOnly, UserRole.Admin, UserRole.ReadWrite)
  @ApiSecurity("apiKey")
  @Post("insertMany")
  @ApiOperation({
    summary: "Insert Documents",
    description: "Insert multiple documents into a collection.",
  })
  @ApiCreatedResponse({
    description: "The result of an insertMany operation.",
    type: InsertManyResponseBody,
  })
  @ApiBadRequestResponse({
    description: ErrorMessages.INVALID_REQUEST,
    type: ErrorResponse,
  })
  @ApiUnauthorizedResponse({
    description: ErrorMessages.UNAUTHORIZED_REQUEST,
    type: ErrorResponse,
  })
  @ApiForbiddenResponse({
    description: ErrorMessages.FORBIDDEN_REQUEST,
    type: ErrorResponse,
  })
  async insertMany(@Body() insertManyRequest: InsertManyRequestBody) {
    return await this.actionService.insertMany(insertManyRequest)
  }

  @Roles(UserRole.ReadOnly, UserRole.Admin, UserRole.ReadWrite)
  @ApiSecurity("apiKey")
  @Post("findOne")
  @HttpCode(200)
  @ApiOperation({
    summary: "Find One Document",
    description: "Find a single document that matches a query.",
  })
  @ApiOkResponse({
    description: "The result of a findOne operation.",
    type: FindOneResponseBody,
  })
  @ApiBadRequestResponse({
    description: ErrorMessages.INVALID_REQUEST,
    type: ErrorResponse,
  })
  @ApiUnauthorizedResponse({
    description: ErrorMessages.UNAUTHORIZED_REQUEST,
    type: ErrorResponse,
  })
  @ApiForbiddenResponse({
    description: ErrorMessages.FORBIDDEN_REQUEST,
    type: ErrorResponse,
  })
  findOne(@Body() findOneRequest: FindOneRequestBody) {
    return this.actionService.findOne(findOneRequest)
  }

  @Roles(UserRole.ReadOnly, UserRole.Admin, UserRole.ReadWrite)
  @ApiSecurity("apiKey")
  @Post("find")
  @HttpCode(200)
  @ApiOperation({
    summary: "Find Documents",
    description: "Find multiple documents that match a query.",
  })
  @ApiOkResponse({
    description: "The result of a find operation.",
    type: FindResponseBody,
  })
  @ApiBadRequestResponse({
    description: ErrorMessages.INVALID_REQUEST,
    type: ErrorResponse,
  })
  @ApiUnauthorizedResponse({
    description: ErrorMessages.UNAUTHORIZED_REQUEST,
    type: ErrorResponse,
  })
  @ApiForbiddenResponse({
    description: ErrorMessages.FORBIDDEN_REQUEST,
    type: ErrorResponse,
  })
  find(@Body() findRequest: FindRequestBody) {
    return this.actionService.find(findRequest)
  }

  @Roles(UserRole.WriteOnly, UserRole.Admin, UserRole.ReadWrite)
  @ApiSecurity("apiKey")
  @Post("updateOne")
  @HttpCode(200)
  @ApiOperation({
    summary: "Update One Document",
    description: "Update a single document in a collection.",
  })
  @ApiOkResponse({
    description: "The result of a updateOne operation.",
    type: UpdateResponseBody,
  })
  @ApiBadRequestResponse({
    description: ErrorMessages.INVALID_REQUEST,
    type: ErrorResponse,
  })
  @ApiUnauthorizedResponse({
    description: ErrorMessages.UNAUTHORIZED_REQUEST,
    type: ErrorResponse,
  })
  @ApiForbiddenResponse({
    description: ErrorMessages.FORBIDDEN_REQUEST,
    type: ErrorResponse,
  })
  updateOne(@Body() updateRequest: UpdateRequestBody) {
    return this.actionService.updateOne(updateRequest)
  }

  @Roles(UserRole.WriteOnly, UserRole.Admin, UserRole.ReadWrite)
  @ApiSecurity("apiKey")
  @Post("update")
  @HttpCode(200)
  @ApiOperation({
    summary: "Update Documents",
    description: "Update multiple documents in a collection.",
  })
  @ApiOkResponse({
    description: "The result of a updateMany operation.",
    type: UpdateResponseBody,
  })
  @ApiBadRequestResponse({
    description: ErrorMessages.INVALID_REQUEST,
    type: ErrorResponse,
  })
  @ApiUnauthorizedResponse({
    description: ErrorMessages.UNAUTHORIZED_REQUEST,
    type: ErrorResponse,
  })
  @ApiForbiddenResponse({
    description: ErrorMessages.FORBIDDEN_REQUEST,
    type: ErrorResponse,
  })
  update(@Body() updateRequest: UpdateRequestBody) {
    return this.actionService.update(updateRequest)
  }

  @Roles(UserRole.WriteOnly, UserRole.Admin, UserRole.ReadWrite)
  @ApiSecurity("apiKey")
  @Post("deleteOne")
  @HttpCode(200)
  @ApiOperation({
    summary: "Delete One Document",
    description: "Delete a single document in a collection.",
  })
  @ApiOkResponse({
    description: "The result of a deleteOne operation.",
    type: DeleteResponseBody,
  })
  @ApiBadRequestResponse({
    description: ErrorMessages.INVALID_REQUEST,
    type: ErrorResponse,
  })
  @ApiUnauthorizedResponse({
    description: ErrorMessages.UNAUTHORIZED_REQUEST,
    type: ErrorResponse,
  })
  @ApiForbiddenResponse({
    description: ErrorMessages.FORBIDDEN_REQUEST,
    type: ErrorResponse,
  })
  deleteOne(@Body() deleteRequest: DeleteRequestBody) {
    return this.actionService.deleteOne(deleteRequest)
  }

  @Roles(UserRole.WriteOnly, UserRole.Admin, UserRole.ReadWrite)
  @ApiSecurity("apiKey")
  @Post("delete")
  @HttpCode(200)
  @ApiOperation({
    summary: "Delete Documents",
    description: "Delete multiple documents in a collection.",
  })
  @ApiOkResponse({
    description: "The result of a delete operation.",
    type: DeleteResponseBody,
  })
  @ApiBadRequestResponse({
    description: ErrorMessages.INVALID_REQUEST,
    type: ErrorResponse,
  })
  @ApiUnauthorizedResponse({
    description: ErrorMessages.UNAUTHORIZED_REQUEST,
    type: ErrorResponse,
  })
  @ApiForbiddenResponse({
    description: ErrorMessages.FORBIDDEN_REQUEST,
    type: ErrorResponse,
  })
  delete(@Body() deleteRequest: DeleteRequestBody) {
    return this.actionService.delete(deleteRequest)
  }

  @Roles(UserRole.ReadOnly, UserRole.Admin, UserRole.ReadWrite)
  @ApiSecurity("apiKey")
  @Post("aggregate")
  @HttpCode(200)
  @ApiOperation({
    summary: "Aggregate Documents",
    description: "Run an aggregation pipeline.",
  })
  @ApiOkResponse({
    description: "The result of a aggregate operation.",
    type: AggregateResponseBody,
  })
  @ApiBadRequestResponse({
    description: ErrorMessages.INVALID_REQUEST,
    type: ErrorResponse,
  })
  @ApiUnauthorizedResponse({
    description: ErrorMessages.UNAUTHORIZED_REQUEST,
    type: ErrorResponse,
  })
  @ApiForbiddenResponse({
    description: ErrorMessages.FORBIDDEN_REQUEST,
    type: ErrorResponse,
  })
  aggregate(@Body() aggregateRequest: AggregateRequestBody) {
    return this.actionService.aggregate(aggregateRequest)
  }
}
