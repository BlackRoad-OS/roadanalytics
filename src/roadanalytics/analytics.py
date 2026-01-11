"""
RoadAnalytics - Analytics Engine for BlackRoad
Event tracking, metrics, funnels, and cohort analysis.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import hashlib
import json
import logging
import math
import statistics
import threading
import uuid

logger = logging.getLogger(__name__)


class EventType(str, Enum):
    """Event types."""
    PAGE_VIEW = "page_view"
    CLICK = "click"
    FORM_SUBMIT = "form_submit"
    PURCHASE = "purchase"
    SIGNUP = "signup"
    LOGIN = "login"
    CUSTOM = "custom"


class AggregationType(str, Enum):
    """Aggregation types."""
    COUNT = "count"
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    UNIQUE = "unique"
    PERCENTILE = "percentile"


@dataclass
class Event:
    """An analytics event."""
    id: str
    event_type: str
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)
    properties: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "type": self.event_type,
            "user_id": self.user_id,
            "session_id": self.session_id,
            "timestamp": self.timestamp.isoformat(),
            "properties": self.properties
        }


@dataclass
class Metric:
    """A computed metric."""
    name: str
    value: float
    unit: str = ""
    period_start: Optional[datetime] = None
    period_end: Optional[datetime] = None
    dimensions: Dict[str, str] = field(default_factory=dict)


@dataclass
class FunnelStep:
    """A funnel step definition."""
    name: str
    event_type: str
    filters: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FunnelResult:
    """Funnel analysis result."""
    steps: List[str]
    counts: List[int]
    conversion_rates: List[float]
    overall_conversion: float
    drop_offs: List[int]


@dataclass
class CohortDefinition:
    """Cohort definition."""
    name: str
    dimension: str  # e.g., "signup_date"
    granularity: str = "day"  # day, week, month


@dataclass
class CohortResult:
    """Cohort analysis result."""
    cohorts: Dict[str, Dict[int, float]]  # cohort -> {period -> retention rate}
    periods: List[int]


class EventStore:
    """Store for analytics events."""

    def __init__(self, max_events: int = 1000000):
        self.events: List[Event] = []
        self.max_events = max_events
        self.events_by_user: Dict[str, List[Event]] = {}
        self.events_by_type: Dict[str, List[Event]] = {}
        self._lock = threading.Lock()

    def add_event(self, event: Event) -> None:
        """Add an event."""
        with self._lock:
            # Enforce max events (simple eviction)
            if len(self.events) >= self.max_events:
                self.events = self.events[self.max_events // 10:]

            self.events.append(event)

            # Index by user
            if event.user_id:
                if event.user_id not in self.events_by_user:
                    self.events_by_user[event.user_id] = []
                self.events_by_user[event.user_id].append(event)

            # Index by type
            if event.event_type not in self.events_by_type:
                self.events_by_type[event.event_type] = []
            self.events_by_type[event.event_type].append(event)

    def query(
        self,
        event_type: str = None,
        user_id: str = None,
        start_time: datetime = None,
        end_time: datetime = None,
        filters: Dict[str, Any] = None
    ) -> List[Event]:
        """Query events."""
        # Start with appropriate index
        if user_id and user_id in self.events_by_user:
            events = self.events_by_user[user_id]
        elif event_type and event_type in self.events_by_type:
            events = self.events_by_type[event_type]
        else:
            events = self.events

        results = []

        for event in events:
            # Apply filters
            if event_type and event.event_type != event_type:
                continue
            if user_id and event.user_id != user_id:
                continue
            if start_time and event.timestamp < start_time:
                continue
            if end_time and event.timestamp > end_time:
                continue
            if filters:
                match = all(
                    event.properties.get(k) == v
                    for k, v in filters.items()
                )
                if not match:
                    continue

            results.append(event)

        return results

    def count(self, event_type: str = None, **kwargs) -> int:
        """Count events."""
        return len(self.query(event_type=event_type, **kwargs))


class MetricsCalculator:
    """Calculate metrics from events."""

    def __init__(self, store: EventStore):
        self.store = store

    def aggregate(
        self,
        events: List[Event],
        aggregation: AggregationType,
        field: str = None
    ) -> float:
        """Aggregate events."""
        if aggregation == AggregationType.COUNT:
            return float(len(events))

        if not field:
            return 0

        values = [
            float(e.properties.get(field, 0))
            for e in events
            if field in e.properties
        ]

        if not values:
            return 0

        if aggregation == AggregationType.SUM:
            return sum(values)
        elif aggregation == AggregationType.AVG:
            return statistics.mean(values)
        elif aggregation == AggregationType.MIN:
            return min(values)
        elif aggregation == AggregationType.MAX:
            return max(values)
        elif aggregation == AggregationType.UNIQUE:
            return float(len(set(values)))

        return 0

    def time_series(
        self,
        event_type: str,
        aggregation: AggregationType,
        field: str = None,
        granularity: str = "day",
        start_time: datetime = None,
        end_time: datetime = None
    ) -> Dict[str, float]:
        """Generate time series data."""
        events = self.store.query(
            event_type=event_type,
            start_time=start_time,
            end_time=end_time
        )

        # Group by time bucket
        buckets: Dict[str, List[Event]] = {}

        for event in events:
            if granularity == "hour":
                key = event.timestamp.strftime("%Y-%m-%d %H:00")
            elif granularity == "day":
                key = event.timestamp.strftime("%Y-%m-%d")
            elif granularity == "week":
                key = event.timestamp.strftime("%Y-W%W")
            elif granularity == "month":
                key = event.timestamp.strftime("%Y-%m")
            else:
                key = event.timestamp.strftime("%Y-%m-%d")

            if key not in buckets:
                buckets[key] = []
            buckets[key].append(event)

        # Aggregate each bucket
        result = {}
        for key, bucket_events in sorted(buckets.items()):
            result[key] = self.aggregate(bucket_events, aggregation, field)

        return result

    def group_by(
        self,
        events: List[Event],
        dimension: str,
        aggregation: AggregationType = AggregationType.COUNT,
        field: str = None
    ) -> Dict[str, float]:
        """Group and aggregate by dimension."""
        groups: Dict[str, List[Event]] = {}

        for event in events:
            value = event.properties.get(dimension, "unknown")
            key = str(value)

            if key not in groups:
                groups[key] = []
            groups[key].append(event)

        return {
            key: self.aggregate(group_events, aggregation, field)
            for key, group_events in groups.items()
        }


class FunnelAnalyzer:
    """Analyze conversion funnels."""

    def __init__(self, store: EventStore):
        self.store = store

    def analyze(
        self,
        steps: List[FunnelStep],
        start_time: datetime = None,
        end_time: datetime = None,
        window_hours: int = 24
    ) -> FunnelResult:
        """Analyze a funnel."""
        # Get users who completed first step
        first_step = steps[0]
        first_events = self.store.query(
            event_type=first_step.event_type,
            start_time=start_time,
            end_time=end_time,
            filters=first_step.filters
        )

        user_first_times: Dict[str, datetime] = {}
        for event in first_events:
            if event.user_id:
                if event.user_id not in user_first_times:
                    user_first_times[event.user_id] = event.timestamp

        step_names = [s.name for s in steps]
        step_counts = [len(user_first_times)]
        step_users: Set[str] = set(user_first_times.keys())

        # Track completion of each subsequent step
        for i, step in enumerate(steps[1:], 1):
            step_events = self.store.query(
                event_type=step.event_type,
                start_time=start_time,
                end_time=end_time,
                filters=step.filters
            )

            completed_users = set()
            for event in step_events:
                if event.user_id in step_users:
                    first_time = user_first_times.get(event.user_id)
                    if first_time:
                        # Check if within conversion window
                        if event.timestamp >= first_time:
                            if (event.timestamp - first_time).total_seconds() <= window_hours * 3600:
                                completed_users.add(event.user_id)

            step_users = completed_users
            step_counts.append(len(step_users))

        # Calculate conversion rates
        conversion_rates = []
        for i in range(len(step_counts)):
            if i == 0:
                conversion_rates.append(1.0)
            else:
                prev_count = step_counts[i - 1]
                rate = step_counts[i] / prev_count if prev_count > 0 else 0
                conversion_rates.append(rate)

        # Calculate drop-offs
        drop_offs = []
        for i in range(len(step_counts) - 1):
            drop_offs.append(step_counts[i] - step_counts[i + 1])

        overall = step_counts[-1] / step_counts[0] if step_counts[0] > 0 else 0

        return FunnelResult(
            steps=step_names,
            counts=step_counts,
            conversion_rates=conversion_rates,
            overall_conversion=overall,
            drop_offs=drop_offs
        )


class CohortAnalyzer:
    """Analyze cohort retention."""

    def __init__(self, store: EventStore):
        self.store = store

    def analyze(
        self,
        cohort_event: str,  # Event that defines cohort (e.g., "signup")
        retention_event: str,  # Event to track for retention
        periods: int = 7,
        granularity: str = "day"
    ) -> CohortResult:
        """Analyze cohort retention."""
        # Get cohort defining events
        cohort_events = self.store.query(event_type=cohort_event)

        # Group users by cohort
        user_cohorts: Dict[str, str] = {}  # user_id -> cohort_key
        cohort_users: Dict[str, Set[str]] = {}  # cohort_key -> user_ids

        for event in cohort_events:
            if not event.user_id:
                continue

            if granularity == "day":
                key = event.timestamp.strftime("%Y-%m-%d")
            elif granularity == "week":
                key = event.timestamp.strftime("%Y-W%W")
            elif granularity == "month":
                key = event.timestamp.strftime("%Y-%m")
            else:
                key = event.timestamp.strftime("%Y-%m-%d")

            if event.user_id not in user_cohorts:
                user_cohorts[event.user_id] = key

                if key not in cohort_users:
                    cohort_users[key] = set()
                cohort_users[key].add(event.user_id)

        # Get retention events
        retention_events = self.store.query(event_type=retention_event)

        # Build user activity by period
        user_activity: Dict[str, Set[str]] = {}  # user_id -> set of period keys

        for event in retention_events:
            if not event.user_id:
                continue

            if granularity == "day":
                key = event.timestamp.strftime("%Y-%m-%d")
            elif granularity == "week":
                key = event.timestamp.strftime("%Y-W%W")
            elif granularity == "month":
                key = event.timestamp.strftime("%Y-%m")
            else:
                key = event.timestamp.strftime("%Y-%m-%d")

            if event.user_id not in user_activity:
                user_activity[event.user_id] = set()
            user_activity[event.user_id].add(key)

        # Calculate retention for each cohort
        cohorts_result: Dict[str, Dict[int, float]] = {}

        sorted_cohort_keys = sorted(cohort_users.keys())

        for cohort_key in sorted_cohort_keys[-periods:]:
            users = cohort_users[cohort_key]
            cohort_size = len(users)

            if cohort_size == 0:
                continue

            cohorts_result[cohort_key] = {0: 1.0}  # Period 0 is always 100%

            # Check subsequent periods
            for period in range(1, periods):
                # Calculate target period key
                # This is simplified - in production, properly calculate date offset
                active_count = 0
                for user_id in users:
                    activity = user_activity.get(user_id, set())
                    # Check if user was active in any subsequent period
                    if len(activity) > 1:  # Simplified check
                        active_count += 1

                retention_rate = active_count / cohort_size
                cohorts_result[cohort_key][period] = retention_rate

        return CohortResult(
            cohorts=cohorts_result,
            periods=list(range(periods))
        )


class AnalyticsManager:
    """High-level analytics management."""

    def __init__(self):
        self.store = EventStore()
        self.metrics = MetricsCalculator(self.store)
        self.funnels = FunnelAnalyzer(self.store)
        self.cohorts = CohortAnalyzer(self.store)
        self._user_properties: Dict[str, Dict[str, Any]] = {}

    def track(
        self,
        event_type: str,
        user_id: str = None,
        session_id: str = None,
        properties: Dict[str, Any] = None,
        **metadata
    ) -> Event:
        """Track an event."""
        event = Event(
            id=str(uuid.uuid4()),
            event_type=event_type,
            user_id=user_id,
            session_id=session_id,
            properties=properties or {},
            metadata=metadata
        )

        self.store.add_event(event)
        logger.debug(f"Tracked event: {event_type} for user {user_id}")

        return event

    def identify(self, user_id: str, properties: Dict[str, Any]) -> None:
        """Identify/update user properties."""
        if user_id not in self._user_properties:
            self._user_properties[user_id] = {}
        self._user_properties[user_id].update(properties)

    def page_view(self, user_id: str, url: str, **properties) -> Event:
        """Track page view."""
        return self.track(
            EventType.PAGE_VIEW.value,
            user_id=user_id,
            properties={"url": url, **properties}
        )

    def purchase(
        self,
        user_id: str,
        amount: float,
        product_id: str = None,
        **properties
    ) -> Event:
        """Track purchase."""
        return self.track(
            EventType.PURCHASE.value,
            user_id=user_id,
            properties={"amount": amount, "product_id": product_id, **properties}
        )

    def count(self, event_type: str, **filters) -> int:
        """Count events."""
        return self.store.count(event_type, **filters)

    def metric(
        self,
        event_type: str,
        aggregation: AggregationType = AggregationType.COUNT,
        field: str = None,
        **filters
    ) -> float:
        """Calculate a metric."""
        events = self.store.query(event_type=event_type, **filters)
        return self.metrics.aggregate(events, aggregation, field)

    def time_series(
        self,
        event_type: str,
        aggregation: AggregationType = AggregationType.COUNT,
        field: str = None,
        granularity: str = "day",
        days: int = 30
    ) -> Dict[str, float]:
        """Get time series data."""
        start_time = datetime.now() - timedelta(days=days)
        return self.metrics.time_series(
            event_type, aggregation, field, granularity, start_time
        )

    def breakdown(
        self,
        event_type: str,
        dimension: str,
        aggregation: AggregationType = AggregationType.COUNT
    ) -> Dict[str, float]:
        """Break down metrics by dimension."""
        events = self.store.query(event_type=event_type)
        return self.metrics.group_by(events, dimension, aggregation)

    def funnel(self, steps: List[Dict[str, Any]], **kwargs) -> FunnelResult:
        """Analyze a funnel."""
        funnel_steps = [
            FunnelStep(
                name=s["name"],
                event_type=s["event"],
                filters=s.get("filters", {})
            )
            for s in steps
        ]
        return self.funnels.analyze(funnel_steps, **kwargs)

    def retention(
        self,
        cohort_event: str,
        retention_event: str,
        periods: int = 7
    ) -> CohortResult:
        """Analyze retention."""
        return self.cohorts.analyze(cohort_event, retention_event, periods)


# Example usage
def example_usage():
    """Example analytics usage."""
    analytics = AnalyticsManager()

    # Track events
    for i in range(100):
        user_id = f"user-{i % 10}"

        analytics.track("signup", user_id=user_id, properties={"source": "organic"})
        analytics.page_view(user_id, "/home")
        analytics.page_view(user_id, "/products")

        if i % 3 == 0:
            analytics.track("add_to_cart", user_id=user_id, properties={"product": "widget"})

        if i % 5 == 0:
            analytics.purchase(user_id, amount=29.99, product_id="widget-1")

    # Count events
    print(f"Total signups: {analytics.count('signup')}")
    print(f"Total purchases: {analytics.count('purchase')}")

    # Metrics
    revenue = analytics.metric("purchase", AggregationType.SUM, "amount")
    avg_order = analytics.metric("purchase", AggregationType.AVG, "amount")
    print(f"Total revenue: ${revenue:.2f}")
    print(f"Avg order value: ${avg_order:.2f}")

    # Breakdown
    breakdown = analytics.breakdown("page_view", "url")
    print(f"\nPage views by URL: {breakdown}")

    # Funnel
    funnel = analytics.funnel([
        {"name": "Signup", "event": "signup"},
        {"name": "View Products", "event": "page_view", "filters": {"url": "/products"}},
        {"name": "Add to Cart", "event": "add_to_cart"},
        {"name": "Purchase", "event": "purchase"}
    ])
    print(f"\nFunnel: {funnel.counts}")
    print(f"Overall conversion: {funnel.overall_conversion:.1%}")

