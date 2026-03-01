# SnapFare – System Overview & Technical Documentation

Last updated: 2025-12-31

## Purpose of this document
This document explains how the SnapFare system works end-to-end.
It is intended for:
- the project owner
- freelancers / external developers
- future maintainers

The goal is to ensure changes can be made safely without breaking
legal compliance, tracking, or core business logic.

---

## 1. System principles

### What SnapFare does
SnapFare changes how people book travel by delivering personalised flight and travel deals
and enabling users to search and book flights in under a minute.
The system focuses on understanding what users care about most, sourcing the best available
deals, distributing them efficiently, and combining this with a fast, opinionated flight search
experience.

---

### Core system principles (non-negotiable)

The following principles must always be respected when making changes to the system:

1. **Supabase is the single source of truth (especially the subscribers table)**
   - All user state (signup status, consent, subscription tier, payment state) lives in Supabase.
   - External tools (email providers, tracking tools) mirror state but never own it.

2. **Consent before action**
   - No marketing emails are sent without confirmed double opt-in.
   - No tracking or advertising scripts are loaded without explicit user consent.
   - Legal compliance is as important as growth or optimisation.

3. **Separation of concerns**
   - Supabase: business logic and state
   - Resend: transactional and confirmation emails
   - Email provider (e.g. EmailOctopus): newsletter delivery only
   - Frontend: UX and user interaction only

4. **Idempotent backend logic**
   - Backend functions must be safe to call multiple times.
   - Retrying a request must not create duplicate users, payments, or emails.

5. **Unsubscribe is a hard stop**
   - If a user unsubscribes, the system must respect this across all tools.
   - Unsubscribed users must not be re-added automatically.

6. **Future-provider flexibility**
   - No critical logic should depend on a single external provider.
   - Tools must be replaceable without changing core flows.


## 2. Signup & consent flow

This section documents how users sign up, how consent is collected, and when a user is
considered active in the system. This flow is legally sensitive and must not be changed
without careful review.

---

### Overview

The signup flow is based on a **double opt-in** model.

A user is only considered subscribed once they have explicitly confirmed their email
address. Until then, they must not receive marketing emails or be added to newsletter
distribution lists.

---

### Signup states

A subscriber can be in one of the following states:

- `pending`  
  The user has submitted their email address but has not confirmed it yet.

- `active`  
  The user has confirmed their email address and may receive newsletters and deals.

- `unsubscribed`  
  The user has opted out and must not receive any further emails.

These states are stored in the `subscribers` table in Supabase and represent the single
source of truth.

---

### Step-by-step flow

1. **User submits email on the website**
   - The frontend sends the email to the confirmation email function.
   - No tracking or newsletter subscription happens at this point.

2. **Confirmation email is sent**
   - A unique confirmation token is generated.
   - The subscriber is stored or updated in Supabase with:
     - `status = 'pending'`
     - `confirm_token`
     - `confirm_expires_at`
   - A confirmation email is sent via Resend.
   - This email contains **only** a confirmation link and no deals.

3. **User clicks confirmation link**
   - The link points to the `/confirm` endpoint.
   - The confirmation token and expiry are validated.

4. **Subscriber is activated**
   - If the token is valid:
     - `status` is set to `active`
     - `confirmed_at` is set
     - confirmation token fields are cleared
   - The user is now considered subscribed.

5. **Newsletter onboarding**
   - After activation, the user is added to the Free newsletter list
     in the email delivery provider.
   - A welcome / deals email is sent after confirmation.

---

### Important rules

- Users must **never** be added to newsletter lists before confirmation.
- Confirmation emails must not include marketing or deal content.
- Email providers do not own consent; they only receive confirmed users.
- Retrying the signup or confirmation flow must not create duplicate users later on.

---

### Failure and retry behavior

- If a confirmation token expires:
  - The user remains in `pending` state.
  - A new confirmation email may be sent.
- If a user signs up again with the same email:
  - The existing subscriber record is updated.
  - No duplicate records are created.


## 3. Email & newsletter architecture

This section explains how emails are structured, which types of emails exist, and which
systems are responsible for sending them. The goal is to keep email delivery reliable,
legally compliant, and provider-independent.

---

### Email types

The system distinguishes strictly between **transactional** and **marketing** emails.

#### Transactional emails
Transactional emails are sent as a direct result of a user action and do not require
newsletter consent.

Examples:
- Confirmation email (double opt-in)
- Premium upgrade / payment confirmation
- Critical account-related messages

Transactional emails are sent via **Resend**.

---

#### Marketing emails
Marketing emails include:
- Flight and travel deals
- Newsletters
- Promotional content

Marketing emails are only sent to users with:
- `status = 'active'`
- and who are not `unsubscribed`

Marketing emails are delivered via the newsletter provider.

---

### Double opt-in and email timing

The email flow is intentionally split into two steps:

1. **Confirmation email**
   - Sent immediately after signup
   - Contains only a confirmation link
   - No deals, promotions, or marketing language

2. **Welcome / deals email**
   - Sent only after successful confirmation
   - May include curated flight deals and onboarding content

This separation ensures legal compliance and clean consent handling.

---

### Newsletter provider role

The newsletter provider is used **only** for email distribution.

Important rules:
- The provider must never decide who is subscribed.
- Only users confirmed in Supabase may be added.
- The provider mirrors state from Supabase.

This design allows the newsletter provider to be replaced in the future without changing
core signup or consent logic.

---

### Premium users

Premium users are handled as a subset of confirmed users.

Rules:
- Premium upgrades are driven by payment logic, not email tools.
- Users are moved from Free to Premium lists after payment confirmation.
- Premium users must not receive Free newsletters unless explicitly intended.

---

### Unsubscribe handling

- Unsubscribes are initiated through the newsletter provider.
- Unsubscribe events are synced back to Supabase via webhook.
- Once a user is marked as `unsubscribed`, they must not receive any further marketing emails.

Unsubscribe status is considered a hard stop.

---

### Design constraints

- Email content and logic must not be tightly coupled to a single provider.
- Email delivery failures must not affect business logic.
- Retrying email sends must not change subscription state.


## 4. Tracking & consent

This section documents how user tracking is handled and how consent is enforced.
Tracking is legally sensitive and must always respect user choice.

---

### Core principle

Tracking is only allowed **after explicit user consent**.

If consent is not granted:
- No tracking scripts are loaded
- No tracking events are fired
- No cookies or similar identifiers are set

This rule applies to all analytics and marketing tools.

---

### Consent model

The system uses a simple, explicit consent model:

- Consent value:
  - `granted`
  - `denied`

- Consent is stored client-side (e.g. localStorage or cookies).
- If no consent value exists, the consent banner is shown.

---

### Consent banner behavior

- Displayed on first visit if no consent decision exists.
- Offers two options:
  - Accept tracking
  - Decline tracking
- The banner disappears permanently once a choice is made.
- Users must later on be able to change their choice later (e.g. via footer link).

---

### Tracking tools

Tracking tools may include:
- Analytics tools (e.g. Google Analytics)
- Advertising pixels (e.g. TikTok Pixel)
- Affiliate tracking links

All tracking tools are treated as **marketing tracking** and require consent.

---

### Gating rules (non-negotiable)

- Tracking scripts must **not** be included statically in the page.
- Tracking scripts must be loaded **dynamically** only after consent is granted.
- Tracking events must never fire before consent.

The presence of a banner alone is insufficient; script loading itself must be gated.

---

### Returning visitors

On page load:
- If consent = `granted` → tracking scripts may load immediately.
- If consent = `denied` → tracking scripts must never load.
- If consent is missing → show consent banner.

---

### Relationship to email consent

Website tracking consent and email consent are separate:

- Website tracking consent controls analytics and advertising.
- Email consent is handled via double opt-in during signup.
- One does not imply the other.

---

### Design constraints

- Consent handling must be provider-agnostic.
- Tracking tools must be replaceable without changing consent logic.
- Legal compliance takes precedence over attribution accuracy.


## 5. Payment → Premium logic

This section describes how payments are handled and how users are upgraded to Premium.
Payment handling is business-critical and must remain deterministic and auditable.

---

### Core principle

Premium access is granted **only** after a payment is confirmed.
Email tools and frontend state must never determine Premium status.

Supabase is the single source of truth for:
- payment state
- premium entitlement

---

### Payment states

Payments are stored in the `payments` table and use the following states:

- `pending`  
  A payment intent exists, but the payment is not yet confirmed.

- `paid`  
  The payment has been confirmed and Premium access may be granted.

The transition from `pending` → `paid` is the decisive moment.

---

### Premium upgrade flow

1. **Payment is initiated**
   - A row is created in `payments`
   - `status = 'pending'`

2. **Payment is confirmed**
   - Payment provider confirmation or manual reconciliation
   - `payments.status` is updated to `paid`

3. **Premium finalisation**
   - The `mark_paid` backend function is triggered
   - This function is the only place where Premium is granted

---

### `mark_paid` responsibilities

The `mark_paid` function performs all Premium-related side effects:

- Ensures the payment is marked as `paid`
- Upserts the subscriber in Supabase:
  - `tier = 'premium'`
  - `status = 'active'`
- Updates email distribution:
  - removes the user from Free lists
  - adds the user to Premium lists
- Sends a Premium welcome / confirmation email

All of these steps must succeed or be safely retryable.

---

### Idempotency and retries

- `mark_paid` must be safe to call multiple times later on.
- Re-running the function must not:
  - duplicate payments
  - downgrade users
  - send duplicate state changes
- Email sending may be repeated, but subscription state must remain correct.

---

### Relationship to email and tracking

- Premium status is independent of email subscription state.
- Premium users may still unsubscribe from newsletters.
- Tracking of Premium conversions must respect website consent rules.

---

### Design constraints

- Payment logic must not depend on the frontend.
- Email providers must not control Premium access.
- Premium state must be derivable solely from Supabase data.


## 6. Unsubscribe logic

This section documents how unsubscribes are handled across the system.
Unsubscribe handling is legally critical and must always override all other logic.

---

### Core principle

An unsubscribe is a **hard stop**.

Once a user unsubscribes:
- They must not receive any further marketing emails
- They must not be automatically re-added to any newsletter list
- This applies regardless of user tier (Free or Premium)

---

### Source of truth

- The authoritative unsubscribe state is stored in Supabase:
  - `subscribers.status = 'unsubscribed'`
  - `subscribers.unsubscribed_at` is set

- Email providers do not own unsubscribe state.
  They only report unsubscribe events.

---

### Unsubscribe flow (going forward)

1. **User unsubscribes via email**
   - The user clicks an unsubscribe link in a newsletter email.
   - The action is handled by the newsletter provider.

2. **Webhook notification**
   - The newsletter provider sends an unsubscribe webhook event.
   - The webhook is received by the `eo_unsubscribe` backend function.

3. **Supabase update**
   - The subscriber record is updated:
     - `status = 'unsubscribed'`
     - `unsubscribed_at = now()`

This ensures Supabase stays in sync with real-world user intent.

---

### Behavior after unsubscribe

Once a user is unsubscribed:

- They must not be added to newsletter lists during:
  - confirmation flows
  - Premium upgrades
  - retries or replays of backend logic
- Backend functions must check unsubscribe status before performing email-related actions.

Unsubscribe state always overrides:
- confirmation status
- subscription tier
- payment state

---

### Re-subscription (explicit only)

If re-subscription is supported in the future:
- It must require explicit user action
- It must include a fresh double opt-in
- Unsubscribe state must not be silently reverted

Until explicitly implemented, re-subscription is not allowed later on.

---

### Design constraints

- Unsubscribe handling must be provider-agnostic.
- Webhook failures must not silently re-enable subscriptions.
- Legal compliance takes precedence over user segmentation or revenue logic.


