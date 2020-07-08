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
	private String CONSUMER_ADDRESS = "com.rotem.api";
	@Override
	public void start(Promise<Void> promise) throws Exception {
		vertx.eventBus().consumer(CONSUMER_ADDRESS, this::onMessage);
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
		String filePath = "target/classes/" + username + ".json";

		if (!validateInput(message)) {
			message.fail(403, "INPUT MISSING");
			return;
		} 

		vertx.fileSystem().exists(filePath, result -> {
			if (result.succeeded() && result.result()) {
				vertx.fileSystem().open(filePath, new OpenOptions().setAppend(true), ar -> {
					if (ar.succeeded()) {
						vertx.fileSystem().readFile(filePath, result2 -> {
							if (result2.succeeded()) {
								System.out.println(result2.result());
								JsonArray arr = result2.result().toJsonArray();
								JsonObject toAdd = message.body();
								toAdd.put("orderId", arr.size() + 1);

								arr.add(toAdd);

								vertx.fileSystem().writeFile(filePath, arr.toBuffer(), result1 -> {
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
				JsonObject toAdd = message.body();
				toAdd.put("orderId", 1);
				JsonArray array = new JsonArray().add(toAdd);
				vertx.fileSystem().writeFile(filePath, array.toBuffer(), result1 -> {
					fileWriteDidSuccess(result1, message);
					return;
				});
			}
		});

	}

	private boolean validateInput(Message<JsonObject> message) {
		String username = message.body().getString("username");
		String date = message.body().getString("orderDate");
		String orderName = message.body().getString("orderName");
		if(username == null || username =="" || 
				date == null || date == "" || 
				orderName == null || orderName == "") {
			return false;
		}
		return true;
	}

	private void handleListOrders(Message<JsonObject> message) {
		String username = message.body().getString("username");
		String filePath = "target/classes/" + username + ".json";
		if(username == null || username == "") {
			message.fail(500, "Username is missing");
			return;
		}
		vertx.fileSystem().readFile(filePath, result -> {
			if (result.succeeded()) {
				//				System.out.println(result.result());
				JsonArray content = result.result().toJsonArray();
				message.reply(new JsonObject().put("orders", (Object)content));
			} else {
				System.out.println("Oh oh ..." + result.cause());
				message.fail(500, result.cause().getMessage());
			}
		});
	}


	private void fileWriteDidSuccess(AsyncResult<Void> result, Message<JsonObject> message) {
		if (result.succeeded()) {
			message.reply(new JsonObject().put("status", "orderAdded"));
		} else{
			System.out.println("Oh oh ..." + result.cause());
			message.fail(500, result.cause().getMessage());
		}

	}
}
