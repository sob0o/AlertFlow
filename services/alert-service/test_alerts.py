import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from main import evaluate, alerts

def setup_function():
    alerts.clear()

def test_critical_success_rate():
    evaluate("stripe_eu", {"success_rate": 85.0, "latency": {"p99": 100}})
    assert len(alerts) == 1
    assert alerts[0]["level"] == "CRITICAL"
    assert alerts[0]["merchant_id"] == "stripe_eu"

def test_warning_success_rate():
    evaluate("lydia", {"success_rate": 92.0, "latency": {"p99": 100}})
    assert len(alerts) == 1
    assert alerts[0]["level"] == "WARNING"

def test_critical_p99_latency():
    evaluate("acme_pay", {"success_rate": 99.0, "latency": {"p99": 1500}})
    assert any(a["level"] == "CRITICAL" for a in alerts)

def test_warning_p99_latency():
    evaluate("acme_pay", {"success_rate": 99.0, "latency": {"p99": 600}})
    assert any(a["level"] == "WARNING" for a in alerts)

def test_no_alert_healthy():
    evaluate("healthy_merchant", {"success_rate": 99.9, "latency": {"p99": 200}})
    assert len(alerts) == 0