// Headless check of the ReatMetric configuration this folder belongs to, against
// the groundlink example sle_spacecraft: starts the ReatMetric core (which binds
// RAF and CLTU at start-up), sends TC(17,1) over SLE CLTU and waits until the
// command verification has seen TM(1,1), TM(1,3) and TM(1,7) arrive over SLE RAF.
//
// Run with check.sh next to this file.

import eu.dariolucia.reatmetric.api.IReatmetricRegister;
import eu.dariolucia.reatmetric.api.IReatmetricSystem;
import eu.dariolucia.reatmetric.api.activity.ActivityOccurrenceData;
import eu.dariolucia.reatmetric.api.activity.ActivityOccurrenceReport;
import eu.dariolucia.reatmetric.api.activity.ActivityOccurrenceState;
import eu.dariolucia.reatmetric.api.activity.ActivityReportState;
import eu.dariolucia.reatmetric.api.activity.ActivityRouteAvailability;
import eu.dariolucia.reatmetric.api.activity.ActivityRouteState;
import eu.dariolucia.reatmetric.api.activity.IActivityOccurrenceDataSubscriber;
import eu.dariolucia.reatmetric.api.model.SystemEntityPath;
import eu.dariolucia.reatmetric.api.processing.input.ActivityRequest;

import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class SleCheck {

    private static final int TC_17_1 = 1701;
    private static final String TC_17_1_PATH = "SPACE.GROUNDLINK.PUS17.TC17_1";

    public static void main(String[] args) throws Exception {
        IReatmetricRegister register = ServiceLoader.load(IReatmetricRegister.class).findFirst().orElseThrow();
        IReatmetricSystem system = register.availableSystems().get(0);
        system.initialise(status -> System.out.println("[check] system status: " + status));

        String route = waitForRoute(system);
        if (route == null) {
            System.out.println("[check] FAILED: no TC route became available (is sle_spacecraft running?)");
            system.dispose();
            System.exit(1);
        }

        CountDownLatch completed = new CountDownLatch(1);
        final ActivityOccurrenceData[] last = new ActivityOccurrenceData[1];
        IActivityOccurrenceDataSubscriber subscriber = items -> {
            for (ActivityOccurrenceData item : items) {
                last[0] = item;
                ActivityOccurrenceReport report = item.getProgressReports().get(item.getProgressReports().size() - 1);
                System.out.println("[check] TC(17,1) " + item.getCurrentState() + ": " + report.getName() + " " + report.getStatus());
                if (item.getCurrentState() == ActivityOccurrenceState.COMPLETED) {
                    completed.countDown();
                }
            }
        };
        system.getActivityOccurrenceDataMonitorService().subscribe(subscriber, null);

        System.out.println("[check] sending TC(17,1) on route " + route);
        system.getActivityExecutionService().startActivity(new ActivityRequest(TC_17_1,
                SystemEntityPath.fromString(TC_17_1_PATH), List.of(), Map.of(), route, "SleCheck"));

        boolean done = completed.await(60, TimeUnit.SECONDS);
        boolean ok = done;
        for (String stage : List.of("Ground Station Reception", "Ground Station Uplink", "On-board Acceptance",
                "On-board Start", "On-board Completion")) {
            ok &= last[0] != null && last[0].getProgressReports().stream()
                    .filter(r -> r.getName().equals(stage))
                    .reduce((first, second) -> second)
                    .map(r -> r.getStatus() == ActivityReportState.OK)
                    .orElse(false);
        }
        System.out.println("[check] verification stages:");
        if (last[0] != null) {
            for (ActivityOccurrenceReport r : last[0].getProgressReports()) {
                System.out.println("[check]   " + r.getName() + ": " + r.getStatus());
            }
        }
        System.out.println(ok ? "[check] PASSED" : "[check] FAILED");
        system.dispose();
        System.exit(ok ? 0 : 1);
    }

    /** Waits up to 60 s for an available route for TCs and returns its name. */
    private static String waitForRoute(IReatmetricSystem system) throws Exception {
        for (int i = 0; i < 60; i++) {
            List<ActivityRouteState> routes = system.getActivityExecutionService().getRouteAvailability("TC");
            for (ActivityRouteState r : routes) {
                if (r.getAvailability() == ActivityRouteAvailability.AVAILABLE) {
                    System.out.println("[check] route available: " + r.getRoute());
                    return r.getRoute();
                }
            }
            if (i % 10 == 0) {
                System.out.println("[check] waiting for a TC route: " + routes.stream().map(r -> r.getRoute() + "=" + r.getAvailability()).toList());
            }
            Thread.sleep(1000);
        }
        return null;
    }
}
