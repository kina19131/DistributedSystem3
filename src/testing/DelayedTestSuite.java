package testing;

import testing.M2Test4;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestResult;
import junit.framework.TestSuite;

public class DelayedTestSuite extends TestCase {
    private static final long DELAY_MS = 1000; 

    public static Test suite() {
        TestSuite suite = new TestSuite("Delayed Test Suite");
        
        suite.addTest(new DelayedTestWrapper(M2Test4.class));
        
        return suite;
    }

    private static class DelayedTestWrapper extends TestSuite {
        public DelayedTestWrapper(Class<? extends TestCase> testClass) {
            super(testClass);
        }

        @Override
        public void run(TestResult result) {
            try {
                // Delay before running the test suite
                Thread.sleep(DELAY_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            super.run(result);
        }
    }
}
