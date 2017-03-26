package com.github.lightcopy;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;

public class ApplicationContext extends ResourceConfig {
  public ApplicationContext() {
    register(HelloWorldResource.class);
    property(ServerProperties.METAINF_SERVICES_LOOKUP_DISABLE, true);
  }
}
