"""Factory classes for tenant and user test data."""

import factory
from faker import Faker
from typing import Dict, Any, List, Optional

fake = Faker()


class TenantFactory(factory.Factory):
    """Factory for creating tenant test data."""

    class Meta:
        model = dict

    tenant_id = factory.LazyAttribute(lambda _: fake.uuid4())
    name = factory.LazyAttribute(lambda _: fake.company())
    description = factory.LazyAttribute(lambda _: fake.catch_phrase())
    status = factory.Iterator(["active", "inactive", "suspended", "trial"])

    @factory.post_generation
    def settings(self, create, extracted, **kwargs):
        """Generate tenant settings."""
        if not create:
            return

        if extracted is None:
            settings = {
                "max_users": fake.random_int(min=5, max=100),
                "max_scenarios": fake.random_int(min=10, max=1000),
                "max_storage_gb": fake.random_int(min=10, max=1000),
                "feature_flags": fake.random_elements(
                    elements=[
                        "advanced_analytics", "real_time_data", "api_access",
                        "custom_models", "bulk_operations", "priority_support"
                    ],
                    length=fake.random_int(min=1, max=6),
                    unique=True
                ),
                "data_retention_days": fake.random_int(min=30, max=365),
                "rate_limits": {
                    "requests_per_minute": fake.random_int(min=60, max=1000),
                    "concurrent_scenarios": fake.random_int(min=5, max=50),
                },
            }
        else:
            settings = extracted

        return settings

    @factory.post_generation
    def contact_info(self, create, extracted, **kwargs):
        """Generate tenant contact information."""
        if not create:
            return

        if extracted is None:
            contact_info = {
                "primary_contact": {
                    "name": fake.name(),
                    "email": fake.email(),
                    "phone": fake.phone_number(),
                    "role": fake.job(),
                },
                "billing_contact": {
                    "name": fake.name(),
                    "email": fake.email(),
                    "phone": fake.phone_number(),
                },
                "technical_contact": {
                    "name": fake.name(),
                    "email": fake.email(),
                    "phone": fake.phone_number(),
                },
            }
        else:
            contact_info = extracted

        return contact_info

    @factory.post_generation
    def subscription(self, create, extracted, **kwargs):
        """Generate tenant subscription information."""
        if not create:
            return

        if extracted is None:
            subscription = {
                "plan": fake.random_element(["starter", "professional", "enterprise", "custom"]),
                "billing_cycle": fake.random_element(["monthly", "annual"]),
                "current_period_start": fake.date_time_this_year(),
                "current_period_end": factory.LazyAttribute(
                    lambda obj: fake.date_time_between(
                        start_date=obj.subscription["current_period_start"],
                        end_date="+1y"
                    ) if obj.subscription["billing_cycle"] == "annual" else fake.date_time_between(
                        start_date=obj.subscription["current_period_start"],
                        end_date="+1M"
                    )
                ),
                "auto_renewal": fake.boolean(),
                "payment_method": fake.credit_card_provider(),
            }
        else:
            subscription = extracted

        return subscription


class UserFactory(factory.Factory):
    """Factory for creating user test data."""

    class Meta:
        model = dict

    user_id = factory.LazyAttribute(lambda _: fake.uuid4())
    username = factory.LazyAttribute(lambda _: fake.user_name())
    email = factory.LazyAttribute(lambda _: fake.email())
    first_name = factory.LazyAttribute(lambda _: fake.first_name())
    last_name = factory.LazyAttribute(lambda _: fake.last_name())
    status = factory.Iterator(["active", "inactive", "pending", "suspended"])

    @factory.post_generation
    def roles(self, create, extracted, **kwargs):
        """Generate user roles."""
        if not create:
            return

        if extracted is None:
            roles = fake.random_elements(
                elements=[
                    "admin", "analyst", "viewer", "developer", "manager"
                ],
                length=fake.random_int(min=1, max=3),
                unique=True
            )
        else:
            roles = extracted

        return roles

    @factory.post_generation
    def permissions(self, create, extracted, **kwargs):
        """Generate user permissions."""
        if not create:
            return

        if extracted is None:
            permissions = fake.random_elements(
                elements=[
                    "read_scenarios", "create_scenarios", "update_scenarios", "delete_scenarios",
                    "run_scenarios", "view_results", "export_data", "manage_users",
                    "system_admin", "billing_admin", "api_access"
                ],
                length=fake.random_int(min=3, max=10),
                unique=True
            )
        else:
            permissions = extracted

        return permissions

    @factory.post_generation
    def preferences(self, create, extracted, **kwargs):
        """Generate user preferences."""
        if not create:
            return

        if extracted is None:
            preferences = {
                "theme": fake.random_element(["light", "dark", "auto"]),
                "timezone": fake.timezone(),
                "language": fake.random_element(["en", "es", "fr", "de", "ja", "zh"]),
                "date_format": fake.random_element(["MM/DD/YYYY", "DD/MM/YYYY", "YYYY-MM-DD"]),
                "currency": fake.random_element(["USD", "EUR", "GBP", "JPY", "CAD"]),
                "notifications": {
                    "email": fake.boolean(),
                    "in_app": fake.boolean(),
                    "scenario_complete": fake.boolean(),
                    "system_updates": fake.boolean(),
                },
                "dashboard": {
                    "default_view": fake.random_element(["scenarios", "analytics", "curves", "reports"]),
                    "widgets": fake.random_elements(
                        elements=["recent_scenarios", "performance_metrics", "curve_chart", "alerts"],
                        length=fake.random_int(min=2, max=4),
                        unique=True
                    ),
                },
            }
        else:
            preferences = extracted

        return preferences


class TenantUserFactory(factory.Factory):
    """Factory for creating tenant-user relationship test data."""

    class Meta:
        model = dict

    tenant_id = factory.LazyAttribute(lambda _: fake.uuid4())
    user_id = factory.LazyAttribute(lambda _: fake.uuid4())
    role = factory.Iterator(["owner", "admin", "member", "viewer"])
    status = factory.Iterator(["active", "invited", "suspended"])

    @factory.post_generation
    def joined_at(self, create, extracted, **kwargs):
        """Generate join date."""
        if not create:
            return

        if extracted is None:
            joined_at = fake.date_time_this_year()
        else:
            joined_at = extracted

        return joined_at

    @factory.post_generation
    def invited_by(self, create, extracted, **kwargs):
        """Generate invited by user."""
        if not create:
            return

        if extracted is None:
            invited_by = fake.uuid4() if fake.boolean() else None
        else:
            invited_by = extracted

        return invited_by


class AccessTokenFactory(factory.Factory):
    """Factory for creating access token test data."""

    class Meta:
        model = dict

    token_id = factory.LazyAttribute(lambda _: fake.uuid4())
    user_id = factory.LazyAttribute(lambda _: fake.uuid4())
    tenant_id = factory.LazyAttribute(lambda _: fake.uuid4())
    token_type = factory.Iterator(["bearer", "api_key"])

    @factory.post_generation
    def scopes(self, create, extracted, **kwargs):
        """Generate token scopes."""
        if not create:
            return

        if extracted is None:
            scopes = fake.random_elements(
                elements=[
                    "read:scenarios", "write:scenarios", "read:curves", "write:curves",
                    "read:analytics", "read:reports", "admin:users", "admin:system"
                ],
                length=fake.random_int(min=2, max=6),
                unique=True
            )
        else:
            scopes = extracted

        return scopes

    @factory.post_generation
    def expires_at(self, create, extracted, **kwargs):
        """Generate token expiration."""
        if not create:
            return

        if extracted is None:
            # Tokens expire in 1-24 hours
            expires_at = fake.date_time_between(
                start_date="now",
                end_date="+1d"
            )
        else:
            expires_at = extracted

        return expires_at

    @factory.post_generation
    def metadata(self, create, extracted, **kwargs):
        """Generate token metadata."""
        if not create:
            return

        if extracted is None:
            metadata = {
                "created_at": fake.date_time_this_year(),
                "last_used": fake.date_time_this_year(),
                "ip_address": fake.ipv4(),
                "user_agent": fake.user_agent(),
            }
        else:
            metadata = extracted

        return metadata
