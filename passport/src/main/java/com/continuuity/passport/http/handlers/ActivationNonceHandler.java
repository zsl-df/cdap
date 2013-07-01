/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.http.handlers;


import com.continuuity.common.http.core.HandlerContext;
import com.continuuity.common.http.core.HttpHandler;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.passport.core.exceptions.StaleNonceException;
import com.continuuity.passport.core.service.DataManagementService;
import com.continuuity.passport.core.service.SecurityService;
import com.continuuity.passport.meta.Account;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

/**
 * Defines end points for Account activation based nonce.
 */
@Path("/passport/v1")
@Singleton
public class ActivationNonceHandler extends PassportHandler implements HttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ActivationNonceHandler.class);

  private final DataManagementService dataManagementService;
  private final SecurityService securityService;

  @Inject
  public ActivationNonceHandler(DataManagementService dataManagementService, SecurityService securityService) {
    this.dataManagementService = dataManagementService;
    this.securityService = securityService;
  }

  @GET
  @Produces("text/plain")
  public void status(HttpRequest request, HttpResponder responder){
    responder.sendString(HttpResponseStatus.OK, "OK");
  }

  @Path("generateActivationKey/{id}")
  @GET
  @Produces("application/json")
  public void getActivationNonce(HttpRequest request, HttpResponder responder,
                                     @PathParam("id") String id) {
    requestReceived();
    int nonce = -1;
    try {
      nonce = securityService.getActivationNonce(id);
      if (nonce != -1) {
        requestSuccess();
        responder.sendString(HttpResponseStatus.OK, Utils.getNonceJson(null, nonce));
      } else {
        requestFailed();
        LOG.error(String.format("Could not generate Nonce. Endpoint %s", "GET /passport/v1/generateActivationKey"));
        responder.sendString(HttpResponseStatus.NOT_FOUND, Utils.getNonceJson("Couldn't generate nonce", nonce));
      }
    } catch (RuntimeException e) {
      requestFailed();
      LOG.error(String.format("Could not generate Nonce. Endpoint %s .%s",
                              "GET /passport/v1/generateActivationKey", e.getMessage()));
      responder.sendString(HttpResponseStatus.NOT_FOUND, Utils.getNonceJson("Couldn't generate nonce", nonce));
    }
  }

  @Path("getActivationId/{nonce}")
  @GET
  @Produces("application/json")
  public void getActivationId(HttpRequest request, HttpResponder responder,
                              @PathParam("nonce") int nonce) {
    requestReceived();
    String id = null;
    try {
      id = securityService.getActivationId(nonce);
      if (id != null && !id.isEmpty()) {
        requestSuccess();
        responder.sendString(HttpResponseStatus.OK, Utils.getIdJson(null, id));
      } else {
        requestFailed();
        LOG.error(String.format("Could not get activation Id. Endpoint %s",
                                "GET /passport/v1/getActivationId"));
        responder.sendString(HttpResponseStatus.NOT_FOUND,
                             Utils.getIdJson("ID not found for nonce", id));
      }
    } catch (StaleNonceException e) {
      requestFailed();
      LOG.error(String.format("Could not get activation id. Endpoint %s. %s",
        "GET /passport/v1/getActivationId", e.getMessage()));
      responder.sendString(HttpResponseStatus.NOT_FOUND,
                           Utils.getIdJson("ID not found for nonce", id));
    }
  }

  @Path("generateResetKey/{email_id}")
  @GET
  @Produces("application/json")
  public void getRegenerateResetKey(HttpRequest request, HttpResponder responder,
                                    @PathParam("email_id") String emailId) {
    requestReceived();
    int nonce = -1;
    try {
      nonce = securityService.getResetNonce(emailId);
      if (nonce != -1) {
        requestSuccess();
        responder.sendString(HttpResponseStatus.OK, Utils.getNonceJson(null, nonce));
      } else {
        requestFailed();
        LOG.error(String.format("Could not get reset key. Endpoint %s",
          "GET /passport/v1/generateResetKey/{emailId}"));
        responder.sendString(HttpResponseStatus.NOT_FOUND,
                             Utils.getNonceJson("Couldn't generate resetKey", nonce));
      }
    } catch (RuntimeException e) {
      requestFailed();
      LOG.error(String.format("Could not get reset key. Endpoint %s %s",
        "GET /passport/v1/generateResetKey/{emailId}", e.getMessage()));
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           Utils.getJsonError(String.format("Couldn't generate resetKey for %s", emailId)));
    }
  }

  @Path("resetPassword/{nonce}")
  @POST
  @Produces("application/json")
  @Consumes("application/json")
  public void resetPassword(HttpRequest request, HttpResponder responder,
                            @PathParam("nonce") int nonce, String data) {
    requestReceived();
    Gson gson = new Gson();
    JsonObject jObject = gson.fromJson(data, JsonElement.class).getAsJsonObject();
    String password = jObject.get("password") == null ? null : jObject.get("password").getAsString();

    if (password != null) {
      try {
        Account account = dataManagementService.resetPassword(nonce, password);
        requestSuccess();
        responder.sendString(HttpResponseStatus.OK, account.toString());
      } catch (Exception e) {
        requestFailed(); // Request failed

        LOG.error(String.format("Could not get reset password. Endpoint %s",
          "GET /passport/v1/resetPassword/{nonce}"));
        responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                             Utils.getJson("FAILED", "Failed to get reset the password"));
      }
    } else {
      requestFailed(); // Request failed
      LOG.error(String.format("Bad request. Password empty. Endpoint %s",
                              "GET /passport/v1/resetPassword/{nonce}"));
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           Utils.getJson("FAILED", "Must send password in request"));
    }
  }
  @Override
  public void init(HandlerContext context) {
  }

  @Override
  public void destroy(HandlerContext context) {
  }
}
