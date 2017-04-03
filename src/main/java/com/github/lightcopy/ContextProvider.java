package com.github.lightcopy;

import java.io.InputStream;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

@Path("/")
public class ContextProvider {
  private static final String INDEX_FAVICON_PATH = "static/favicon.ico";
  private static final String INDEX_LOGO_PATH = "static/logo.png";
  private static final String INDEX_CSS_PATH = "static/index.min.css";
  private static final String INDEX_JS_PATH = "static/index.min.js";
  private static final String INDEX_HTML_PATH = "static/index.html";

  @GET
  @Path(INDEX_FAVICON_PATH)
  @Produces("image/x-icon")
  public Response getFavicon() {
    InputStream icon = this.getClass().getClassLoader().getResourceAsStream(INDEX_FAVICON_PATH);
    return Response.ok(icon).build();
  }

  @GET
  @Path(INDEX_LOGO_PATH)
  @Produces("image/png")
  public Response getLogo() {
    InputStream logo = this.getClass().getClassLoader().getResourceAsStream(INDEX_LOGO_PATH);
    return Response.ok(logo).build();
  }

  @GET
  @Path(INDEX_CSS_PATH)
  @Produces("text/css")
  public Response getIndexCSS() {
    InputStream css = this.getClass().getClassLoader().getResourceAsStream(INDEX_CSS_PATH);
    return Response.ok(css).build();
  }

  @GET
  @Path(INDEX_JS_PATH)
  @Produces("text/javascript")
  public Response getIndexJS() {
    InputStream js = this.getClass().getClassLoader().getResourceAsStream(INDEX_JS_PATH);
    return Response.ok(js).build();
  }

  @GET
  @Produces("text/html")
  public Response getIndex() {
    InputStream index = this.getClass().getClassLoader().getResourceAsStream(INDEX_HTML_PATH);
    return Response.ok(index).build();
  }
}
