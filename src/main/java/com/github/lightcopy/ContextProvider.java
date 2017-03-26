package com.github.lightcopy;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

@Path("helloworld")
public class ContextProvider {
  @GET
  @Produces("text/plain")
  public String getHello() {
    return "Hello World!";
  }
}
