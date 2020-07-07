package com.rotem.interviewTask;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;


public class OrderVerticle extends AbstractVerticle  {

	private static final Logger LOGGER = LoggerFactory.getLogger(OrderVerticle.class); 

	@Override
	public void start(Promise<Void> promise) throws Exception {
		vertx.eventBus().consumer("com.rotem.api", this::onMessage);
		promise.complete();
	}

	public void onMessage(Message<JsonObject> message) {
		if (!message.headers().contains("action")) {
			LOGGER.error("No action header specified for message with headers {} and body {}",
					message.headers(), message.body().encodePrettily());
			message.fail(401, "No action header specified");
			return;
		}

		String action = message.headers().get("action");
		switch (action) {
		case "get-orders":
			handleListOrders(message);
			break;
		case "add-order":
			handleAddOrder(message);
			break;
		default:
			message.fail(401, "Bad action: " + action);
		}
	}


	private void handleAddOrder(Message<JsonObject> message) {
		String username = message.body().getString("username");
		//		String orderId = message.body().getString("orderId");
		if (username == null) {
			message.fail(403, "NO USERNAME");
			return;
		} else {
			String filePath = "target/classes/" + username + ".json";
			//			System.out.println("IN ORDERVERTICLE ADD ORDER -> USERNAME *NOT* EMPTY");
			vertx.fileSystem().exists(filePath, result -> {
				if (result.succeeded() && result.result()) {
					vertx.fileSystem().open(filePath, new OpenOptions().setAppend(true), ar -> {
						if (ar.succeeded()) {
							vertx.fileSystem().readFile(filePath, result2 -> {
								if (result2.succeeded()) {
									System.out.println(result2.result());
									JsonArray arr = result2.result().toJsonArray();

									arr.add(message.body());

									vertx.fileSystem().writeFile("target/classes/" + username +".json", arr.toBuffer(), result1 -> {
										fileWriteDidSuccess(result1, message);
										return;
									});
								}
								else {
									System.out.println("Oh oh ..." + result.cause());
									message.fail(500, result.cause().getMessage());
								}
							});
						} else {
							System.err.println("Could not open file");
							message.fail(500, result.cause().getMessage());
						}
					});
				} else {
					JsonArray array = new JsonArray().add(message.body());
					vertx.fileSystem().writeFile("target/classes/" + username +".json", array.toBuffer(), result1 -> {
						fileWriteDidSuccess(result1, message);
						return;
					});
				}
			});
		}
	}

	private void handleListOrders(Message<JsonObject> message) {
		String username = message.body().getString("username");

		vertx.fileSystem().readFile("target/classes/" + username +".json", result -> {
			if (result.succeeded()) {
				//				System.out.println(result.result());
				JsonArray content = result.result().toJsonArray();
				message.reply(new JsonObject().put("orders", (Object)content));
			} else {
				System.out.println("Oh oh ..." + result.cause());
			}
		});
	}
	//
	//	private void sendError(int statusCode, HttpServerResponse response) {
	//		response.setStatusCode(statusCode).end();
	//	}

	private void fileWriteDidSuccess(AsyncResult<Void> result, Message<JsonObject> message) {
		if (result.succeeded()) {
			message.reply(new JsonObject().put("status", "orderAdded"));
		} else{
			System.out.println("Oh oh ..." + result.cause());
			message.fail(500, result.cause().getMessage());
		}

	}
}
