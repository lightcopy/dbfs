package com.github.lightcopy;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;

import com.github.lightcopy.conf.AppConf;

/**
 * Application context.
 * Only manual lookup is enabled, provider discovery is disabled.
 */
public class ApplicationContext extends ResourceConfig {
  private final transient AppConf conf;

  public ApplicationContext(AppConf conf) {
    this.conf = conf;
    register(ContextProvider.class);
    property(ServerProperties.METAINF_SERVICES_LOOKUP_DISABLE, true);
    property(AppConf.WORKING_DIRECTORY, conf.workingDirectory());
  }

  @Path("/")
  public static class ContextProvider {
    // images
    private static final String INDEX_FAVICON_PATH = "favicon.ico";
    private static final String INDEX_LOGO_PATH = "logo.png";
    // html
    private static final String INDEX_HTML_PATH = "index.html";

    @Context
    Configuration config;

    /** Get current working directory from context */
    private File workingDirectory() {
      return new File((String) config.getProperty(AppConf.WORKING_DIRECTORY));
    }

    /** Get static directory with custom resources */
    private File staticDirectory() {
      return new File(workingDirectory(), "static");
    }

    /** Open file path and return input stream */
    private InputStream open(File path) {
      try {
        return new FileInputStream(path);
      } catch (FileNotFoundException err) {
        throw new RuntimeException(err);
      }
    }

    @GET
    @Path(INDEX_FAVICON_PATH)
    @Produces("image/x-icon")
    public Response getFavicon() {
      InputStream icon = open(new File(staticDirectory(), INDEX_FAVICON_PATH));
      return Response.ok(icon).build();
    }

    @GET
    @Path(INDEX_LOGO_PATH)
    @Produces("image/png")
    public Response getLogo() {
      InputStream logo = open(new File(staticDirectory(), INDEX_LOGO_PATH));
      return Response.ok(logo).build();
    }

    @GET
    @Produces("text/html")
    public Response getIndex() {
      InputStream index = open(new File(staticDirectory(), INDEX_HTML_PATH));
      return Response.ok(index).build();
    }
  }
}
