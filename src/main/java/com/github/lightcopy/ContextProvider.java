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
  private static final String INDEX_CSS_PATH = "static/index.min.css";
  // javascript
  private static final String INDEX_JS_PATH = "static/index.min.js";
  // html
  private static final String INDEX_HTML_PATH = "static/index.html";
  // various font resources
  private static final String OCTICONS_TTF_PATH = "static/octicons.ttf";
  private static final String OCTICONS_WOFF_PATH = "static/octicons.woff";
  private static final String OCTICONS_EOT_PATH = "static/octicons.eot";
  private static final String OCTICONS_SVG_PATH = "static/octicons.svg";

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
    InputStream stream = this.getClass().getClassLoader().getResourceAsStream(INDEX_CSS_PATH);
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

  @GET
  @Path(OCTICONS_TTF_PATH)
  @Produces("application/octet-stream")
  public Response getOcticonsTTF() {
    InputStream stream = this.getClass().getClassLoader().getResourceAsStream(OCTICONS_TTF_PATH);
    return Response.ok(stream).build();
  }

  @GET
  @Path(OCTICONS_WOFF_PATH)
  @Produces("application/octet-stream")
  public Response getOcticonsWOFF() {
    InputStream stream = this.getClass().getClassLoader().getResourceAsStream(OCTICONS_WOFF_PATH);
    return Response.ok(stream).build();
  }

  @GET
  @Path(OCTICONS_EOT_PATH)
  @Produces("application/octet-stream")
  public Response getOcticonsEOT() {
    InputStream stream = this.getClass().getClassLoader().getResourceAsStream(OCTICONS_EOT_PATH);
    return Response.ok(stream).build();
  }

  @GET
  @Path(OCTICONS_SVG_PATH)
  @Produces("image/svg+xml")
  public Response getOcticonsSVG(@PathParam("svg") String svg) {
    InputStream stream = this.getClass().getClassLoader().getResourceAsStream(OCTICONS_SVG_PATH);
    return Response.ok(stream).build();
  }
}
