package com.github.lightcopy;

import java.io.InputStream;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

@Path("/")
public class ContextProvider {
  // images
  private static final String INDEX_FAVICON_PATH = "static/favicon.ico";
  private static final String INDEX_LOGO_PATH = "static/logo.png";
  // css files
  private static final String INDEX_CSS_PATH = "static/{css}.css";
  // font resources
  private static final String INDEX_TTF_PATH = "static/{ttf}.ttf";
  private static final String INDEX_WOFF_PATH = "static/{woff}.woff";
  private static final String INDEX_EOT_PATH = "static/{eot}.eot";
  private static final String INDEX_SVG_PATH = "static/{svg}.svg";
  // javascript
  private static final String INDEX_JS_PATH = "static/index.min.js";
  // html
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
  public Response getIndexCSS(@PathParam("css") String css) {
    String filepath = INDEX_CSS_PATH.replace("{css}", css);
    InputStream stream = this.getClass().getClassLoader().getResourceAsStream(filepath);
    return Response.ok(stream).build();
  }

  @GET
  @Path(INDEX_TTF_PATH)
  @Produces("application/octet-stream")
  public Response getIndexTTF(@PathParam("ttf") String ttf) {
    String filepath = INDEX_TTF_PATH.replace("{ttf}", ttf);
    InputStream stream = this.getClass().getClassLoader().getResourceAsStream(filepath);
    return Response.ok(stream).build();
  }

  @GET
  @Path(INDEX_WOFF_PATH)
  @Produces("application/octet-stream")
  public Response getIndexWOFF(@PathParam("woff") String woff) {
    String filepath = INDEX_WOFF_PATH.replace("{woff}", woff);
    InputStream stream = this.getClass().getClassLoader().getResourceAsStream(filepath);
    return Response.ok(stream).build();
  }

  @GET
  @Path(INDEX_EOT_PATH)
  @Produces("application/octet-stream")
  public Response getIndexEOT(@PathParam("eot") String eot) {
    String filepath = INDEX_EOT_PATH.replace("{eot}", eot);
    InputStream stream = this.getClass().getClassLoader().getResourceAsStream(filepath);
    return Response.ok(stream).build();
  }

  @GET
  @Path(INDEX_SVG_PATH)
  @Produces("image/svg+xml")
  public Response getIndexSVG(@PathParam("svg") String svg) {
    String filepath = INDEX_EOT_PATH.replace("{svg}", svg);
    InputStream stream = this.getClass().getClassLoader().getResourceAsStream(filepath);
    return Response.ok(stream).build();
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
