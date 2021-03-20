package ru.burdakov;

import org.apache.camel.CamelContext;
import org.apache.camel.Message;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.support.DefaultMessage;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;


public class Starter {

    public static void main(String[] args) {
        CamelContext camel = new DefaultCamelContext();

        camel.getPropertiesComponent().setLocation("classpath:application.properties");

        DataSource dataSource = new DriverManagerDataSource(
                "jdbc:postgresql://localhost:5432/censored?user=censored&password=censored"
        );

        camel.getRegistry().bind("censored", dataSource);

        ProducerTemplate template = camel.createProducerTemplate();

        try {
            camel.addRoutes(new RouteBuilder() {
                @Override
                public void configure() throws Exception {
                    from("file:{{from}}")
                            .routeId("File processing")
//                            .log(">>>>>> ${body}")
                            .convertBodyTo(String.class)
                            .to("log:?showBody=true&showHeaders=true")
                            .choice()
                            .when(exchange -> ((String) exchange.getIn().getBody()).contains("=a"))
                            .to("file:{{toA}}")
                            .when(exchange -> ((String) exchange.getIn().getBody()).contains("=b"))
                            .to("file:{{toB}}");
                }
            });

            camel.addRoutes(new RouteBuilder() {
                @Override
                public void configure() throws Exception {
                    from("timer:base?period=60000")
                            .routeId("jdbcRoute")
                            .setHeader("key", constant(1))
                            .setBody(simple("select id, name from oogis.channel where id > :?key order by id"))
                            .to("jdbc:censored?useHeadersAsParameters=true")
                            .log(">>>>> ${body}")
                            .process(exchange -> {
                                Message in = exchange.getIn();
                                Object body = in.getBody();

                                DefaultMessage message = new DefaultMessage(exchange);
                                message.setHeaders(in.getHeaders());
                                message.setHeader("rnd", "kek");
                                message.setBody(body.toString() + "\n" + in.getHeaders().toString());

                                exchange.setMessage(message);
                            })
                            .toD("file:/home/stanislav/dev/other/camel-app/files/toB?fileName=${date:now:yyyyMMdd}-${headers.rnd}.txt");
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }

        camel.start();

        template.sendBody(
                "file:/home/stanislav/dev/other/camel-app/files?filename=event-${date:now:yyyyMMdd-HH-mm}.html",
                "<hello>world!</hello>");

        try {
            Thread.sleep(4_000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        camel.stop();
    }

}
