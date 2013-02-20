/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.internal.app.services;

import com.continuuity.WordCountApp;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.app.program.Id;
import com.continuuity.app.services.FlowIdentifier;
import com.continuuity.app.services.RuntimeService;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.zookeeper.InMemoryZookeeper;
import com.continuuity.data.runtime.DataFabricModules;
import com.continuuity.internal.app.program.MDSBasedStore;
import com.continuuity.internal.app.program.StoreModule4Test;
import com.continuuity.internal.app.services.legacy.FlowDefinitionImpl;
import com.google.gson.Gson;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

public class RuntimeThriftServerTest {
  private static MDSBasedStore store;
  private static RuntimeThriftServer server;

  @BeforeClass
  public static void beforeClass() {
    final Injector injector = Guice.createInjector(new DataFabricModules().getInMemoryModules(),
                                                   new StoreModule4Test(),
                                                   new ServicesModule4Test(new CConfiguration()));

    store = injector.getInstance(MDSBasedStore.class);
    server = injector.getInstance(RuntimeThriftServer.class);
  }

  @Test (timeout = 20000)
  public void basicThriftServerSpinUpTest() throws Exception {
    // Adding some data to store
    ApplicationSpecification spec = new WordCountApp().configure();
    Id.Application appId = new Id.Application(new Id.Account("account1"), "application1");
    store.addApplication(appId, spec);

    // ZK is needed to register server
    File tempDir = FileUtils.getTempDirectory();
    new InMemoryZookeeper(tempDir);
    tempDir.deleteOnExit();
    // starting server
    CConfiguration conf = new CConfiguration();
    server.start(new String[]{}, conf);

    // Connecting to server
    String serverAddress = conf.get(Constants.CFG_FLOW_MANAGER_SERVER_ADDRESS,
                                    Constants.DEFAULT_FLOW_MANAGER_SERVER_ADDRESS);

    int serverPort = conf.getInt(Constants.CFG_FLOW_MANAGER_SERVER_PORT,
                                 Constants.DEFAULT_FLOW_MANAGER_SERVER_PORT);

    TTransport transport = new TFramedTransport(
                                                 new TSocket(serverAddress, serverPort));
    transport.open();

    TProtocol protocol = new TBinaryProtocol(transport);
    RuntimeService.Client client = new RuntimeService.Client(protocol);

    // Trying to read *any* data
    FlowIdentifier flowId = new FlowIdentifier("account1", "application1", "WordCountFlow", 0);
    String flowDefJson = client.getFlowDefinition(flowId);
    FlowDefinitionImpl flowDef = new Gson().fromJson(flowDefJson, FlowDefinitionImpl.class);
    Assert.assertEquals(3, flowDef.getFlowlets().size());
  }
}
