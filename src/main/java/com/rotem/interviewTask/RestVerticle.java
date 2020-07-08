package com.rotem.interviewTask;

import com.hazelcast.config.Config;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.Session;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.SessionHandler;
import io.vertx.ext.web.sstore.ClusteredSessionStore;
import io.vertx.ext.web.sstore.LocalSessionStore;
import io.vertx.ext.web.sstore.SessionStore;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;

public class RestVerticle extends AbstractVerticle 
{
	//	private static final Logger LOGGER = LoggerFactory.getLogger(RestVerticle.class); 
	private String EVENTBUS_ADDRESS = "com.rotem.api";
	public static void main( String[] args )
	{
		System.out.println("in REST");
		Vertx vertx = Vertx.vertx();
		Buffer buf = Buffer.buffer("[{\"username\":\"stav\", \"password\":\"1234\"},"
				+ "{\"username\":\"rotem\", \"password\":\"gur\"},"
				+ "{\"username\":\"orna\", \"password\":\"or123\"},"
				+ "{\"username\":\"nehemia\", \"password\":\"0704\"},"
				+ "{\"username\":\"gur\", \"password\":\"rotem\"}]");

		vertx.fileSystem().writeFile("target/classes/users.json", buf, result -> {
			if (!result.succeeded()) {
				System.out.println("FAILED TO CREATE USERS FILE");
				return;
			}
		});

		Config hazelcastConfig = new Config();
		ClusterManager mgr = new HazelcastClusterManager(hazelcastConfig);
		VertxOptions options = new VertxOptions().setClusterManager(mgr);

		Vertx.clusteredVertx(options, res -> {
		  if (res.succeeded()) {
		    Vertx ver = res.result();
		    ver.deployVerticle(new RestVerticle());
			ver.deployVerticle(new OrderVerticle());
		  } else {
		    // failed!
			  return;
		  }
		});
	}

	@Override
	public void start(Future<Void> fut) {
		Router router = Router.router(vertx);
		//		router.errorHandler(500, rc -> {
		//			System.err.println("Handling failure");
		//			Throwable failure = rc.failure();
		//			if (failure != null) {
		//				failure.printStackTrace();
		//			}
		//		});

		router.route().handler(BodyHandler.create());
		//session:
		SessionStore store = ClusteredSessionStore.create(vertx);
		SessionHandler sessionHandler = SessionHandler.create(store);
		router.route().handler(sessionHandler);

		router.post("/orders/:username").handler(this::handleAddOrder);
		router.get("/orders/:username").handler(this::handleListOrders);
		router.post("/login").handler(this::login);
		router.post("/logout").handler(this::logout);
		//		router.get("/").handler(this::login);

		vertx.createHttpServer()
		.requestHandler(router)
		.listen(8080);
	}

	private void handleAddOrder(RoutingContext routingContext) {
		HttpServerResponse response = routingContext.response();
		Session session = routingContext.session();
		if(session.get("isSignedIn") == null) {
			response.setStatusCode(400).end();
			return;
		}

//		System.out.println("IN ADD ORDER");
		JsonObject order = new JsonObject();
		//		System.out.println(routingContext.getBodyAsString());
		String username = routingContext.request().getParam("username");
		JsonObject jsonBody = routingContext.getBodyAsJson();

//		String orderId =  jsonBody.getString("orderId");
		String orderName =  jsonBody.getString("orderName");
		String orderDate =  jsonBody.getString("orderDate");

		order.put("username", username).put("orderName", orderName).put("orderDate", orderDate);

		DeliveryOptions options = new DeliveryOptions().addHeader("action", "add-order");
		vertx.eventBus().request(EVENTBUS_ADDRESS, order, options, reply -> {
			if (reply.succeeded()) {
				response.setStatusCode(200).end();
			}else {
				response.setStatusCode(400).end();
			}
		});
	}

	private void handleListOrders(RoutingContext routingContext) {
		HttpServerResponse response = routingContext.response();
		Session session = routingContext.session();
		if(session.get("isSignedIn") == null) {
			response.setStatusCode(400).end();
			return;
		}
		String name = routingContext.request().getParam("username");
		JsonObject username = new JsonObject();
		username.put("username", name);

		DeliveryOptions options = new DeliveryOptions().addHeader("action", "get-orders");
		vertx.eventBus().request(EVENTBUS_ADDRESS, username, options, reply -> {
			if (reply.succeeded()) {
				routingContext.response().putHeader("content-type", "application/json").end(Json.encodePrettily(reply.result().body()));
			}else {
				response.setStatusCode(400).end();
			}
		});
	}

	private void login(RoutingContext routingContext) {
		JsonObject jsonBody = routingContext.getBodyAsJson();
		//		String username =  jsonBody.getString("username");
		//		String password =  jsonBody.getString("password");
		vertx.fileSystem().readFile("target/classes/users.json", result2 -> {
			if (result2.succeeded()) {
				JsonArray arr = result2.result().toJsonArray();

				boolean contains = false;
				for(int i = 0; i < arr.size(); ++i) {
					if(arr.getValue(i).equals(jsonBody)) {
						contains = true;
						break;
					}
				}
				if(contains) {
					Session session = routingContext.session();
					session.put("isSignedIn", true);
					loginResponse(routingContext, true);
				}else {
					//					System.out.println("DOESNT CONTAIN USER " + jsonBody);
					loginResponse(routingContext, false);
				}
			}else {
				loginResponse(routingContext, false);
			}
		});
	}

	private void loginResponse(RoutingContext routingContext, Boolean loggedIn) {
		HttpServerResponse response = routingContext.response();
		response
		.putHeader("content-type", "boolean")
		.end(loggedIn.toString());
		return;
	}

	private void logout(RoutingContext routingContext) {
		Session session = routingContext.session();
		session.put("isSignedIn", null);
		session.destroy();
		HttpServerResponse response = routingContext.response();
		response
		.putHeader("content-type", "text/html")
		.end("LOGGED OUT");
	}

}


