package it.comune.trieste.ouf.mcp;

import static io.modelcontextprotocol.spec.McpSchema.*;

import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpServerFeatures;
import io.modelcontextprotocol.server.transport.DefaultServerTransportSecurityValidator;
import io.modelcontextprotocol.server.transport.HttpServletStreamableServerTransportProvider;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.catalina.Context;
import org.apache.catalina.startup.Tomcat;

public final class FeasibilityServer {
  private static final AtomicLong DISPATCHES = new AtomicLong();
  private static final Map<String,Object> INPUT_SCHEMA = Map.of(
      "$schema", JSON_SCHEMA_DIALECT_2020_12,
      "type", "object",
      "additionalProperties", false,
      "required", List.of("anchorId", "relationIri", "limit"),
      "properties", Map.of(
          "anchorId", Map.of("type", "string", "format", "uuid"),
          "relationIri", Map.of("type", "string", "minLength", 1),
          "limit", Map.of("type", "integer", "minimum", 1, "maximum", 100),
          "delayMillis", Map.of("type", "integer", "minimum", 0, "maximum", 1500)));

  public static void main(String[] args) throws Exception {
    int port = Integer.parseInt(System.getProperty("port", "18090"));
    var transport = HttpServletStreamableServerTransportProvider.builder()
        .mcpEndpoint("/mcp")
        .keepAliveInterval(Duration.ofSeconds(15))
        .securityValidator(DefaultServerTransportSecurityValidator.builder()
            .allowedOrigin("http://localhost:*")
            .allowedHost("localhost:*")
            .allowedHost("127.0.0.1:*")
            .build())
        .build();
    var server = McpServer.sync(transport)
        .serverInfo("ouf-mcp-feasibility", "0.1.0")
        .capabilities(ServerCapabilities.builder().tools(false).build())
        .toolCall(Tool.builder("urban.object.related_search", INPUT_SCHEMA)
                .description("Feasibility-only typed capability; no backend URL or query language is accepted.")
                .build(),
            (exchange, request) -> {
              long dispatch = DISPATCHES.incrementAndGet();
              Object delay = request.arguments().get("delayMillis");
              if (delay instanceof Number value && value.longValue() > 0) {
                try { Thread.sleep(value.longValue()); } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
              }
              return CallToolResult.builder()
                  .content(List.of(TextContent.builder("{\"status\":\"MOCK\",\"dispatch\":" + dispatch + "}").build()))
                  .isError(false).build();
            })
        .requestTimeout(Duration.ofSeconds(2))
        .build();

    Tomcat tomcat = new Tomcat(); tomcat.setPort(port); tomcat.setBaseDir(System.getProperty("java.io.tmpdir"));
    Context context = tomcat.addContext("", System.getProperty("java.io.tmpdir"));
    var wrapper = context.createWrapper(); wrapper.setName("mcp"); wrapper.setServlet(transport); wrapper.setLoadOnStartup(1); wrapper.setAsyncSupported(true);
    context.addChild(wrapper); context.addServletMappingDecoded("/*", "mcp"); tomcat.getConnector().setAsyncTimeout(3000);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> { try { server.closeGracefully(); tomcat.stop(); tomcat.destroy(); } catch (Exception ignored) {} }));
    tomcat.start(); System.out.println("OUF_MCP_FEASIBILITY_READY port=" + port); tomcat.getServer().await();
  }
}
