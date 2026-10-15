"""What the PHANTA Ghost can help a workshop with.

Each topic is a plain-language answer, optional numbered steps, buttons to the
right page or setup-guide section, and follow-up suggestions. The topic whose
keyword phrases best match the question wins: the longest matched phrase
counts most, so "how many unanswered messages" picks the Inbox topic over the
generic "message" match and "add a booking" beats "booking".

Only real, existing features are described. Live numbers come from the
dashboard queries passed in as `live`; nothing is invented.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable

# (button label, endpoint, url_for kwargs, #anchor)
Link = tuple[str, str, dict, str]

META = ("help.guide", {"name": "meta"})
GOOGLE = ("help.guide", {"name": "google"})


@dataclass(frozen=True)
class Topic:
    key: str
    phrases: tuple[str, ...]
    answer: str | Callable[[dict], str]
    steps: tuple[str, ...] = ()
    links: tuple[Link, ...] = ()
    next: tuple[str, ...] = ()


def _plural(n, word):
    return f"{n} {word}{'' if n == 1 else 's'}"


def _today(live: dict) -> str:
    return (f"Here's today: your dashboard currently has {_plural(live['todays_bookings'], 'booking')} today, "
            f"{_plural(live['vehicles_waiting'], 'vehicle')} waiting or in progress, and "
            f"{_plural(live['overdue_vehicles'], 'overdue booking')}. "
            + (f"There {'is' if live['booking_requests'] == 1 else 'are'} also {_plural(live['booking_requests'], 'booking request')} waiting for you to confirm."
               if live['booking_requests'] else "No booking requests are waiting to be confirmed."))


def _inbox(live: dict) -> str:
    n = live["unanswered_messages"]
    lead = (f"You have {_plural(n, 'customer chat')} waiting for a reply." if n
            else "Good news: no customer chats are waiting for a reply right now.")
    return (lead + " The Inbox shows every WhatsApp chat with your customers. The AI assistant answers "
            "automatically, and every AI reply starts with \"🤖 AI assistant:\" so customers know it isn't a person. "
            "The Inbox refreshes by itself every 10 seconds.")


def _whatsapp(live: dict) -> str:
    status = (live.get("connection_health") or {}).get("status") or "not_connected"
    if status == "connected":
        lead = "Your WhatsApp is connected and working. ✅"
    elif status in ("reconnect_required", "expiring_soon"):
        lead = "Your WhatsApp needs to be reconnected so PHANTA can keep sending messages."
    elif status in ("onboarding", "failed"):
        lead = "Your WhatsApp setup was started but isn't finished yet, so messages can't be sent."
    else:
        lead = "Your WhatsApp isn't connected yet. It takes about 10 minutes."
    return (lead + " Once connected, customers message your WhatsApp number, the AI assistant answers and books them, "
            "and you can read and reply to every chat in the Inbox.")


def _billing(live: dict) -> str:
    status = (live.get("billing_state") or {}).get("subscription_status")
    if status and status != "not_configured":
        lead = f"Your PHANTA subscription is currently: {status.replace('_', ' ')}."
    else:
        lead = "I can't see an active PHANTA subscription for your workshop yet."
    return lead + (" Your billing statement shows your invoices and payments. "
                   "This is only what you pay VANTA for PHANTA. Ad spend on Facebook or Google is paid directly to them from your own card.")


TOPICS: tuple[Topic, ...] = (
    Topic("hello", ("hello", "hi", "hey", "howzit", "good morning", "good afternoon", "good evening", "help", "what can you do", "who are you"),
          "Hi 👋 I'm the PHANTA Ghost, your workshop helper. Ask me how to do something in PHANTA and I'll walk you through it, "
          "or ask for today's numbers. Here are a few things people often ask:",
          next=("What's happening today?", "How do I add a booking?", "How do I connect WhatsApp?", "How do I boost a post?", "What are my results?")),

    Topic("getting_started", ("getting started", "get started", "where do i start", "new here", "how do i use", "how does this work",
                              "not sure how", "set up", "setup", "first time", "onboarding"),
          "Welcome! Here's the best order to get PHANTA working for your workshop:",
          steps=("Read the Meta setup guide and connect WhatsApp, so customers can message and book you.",
                 "Connect your Facebook Page (and Instagram) in Flyer Lady, so you can post specials.",
                 "Check your opening hours in Settings, so bookings only land on days you're open.",
                 "Optional: connect Google Ads and a Meta ad account if you want to run paid ads.",
                 "Then use the Dashboard every morning: today's bookings, confirmations and vehicles in progress."),
          links=(("Meta setup guide", *META, ""), ("Google Ads setup guide", *GOOGLE, ""), ("Opening hours", "settings.settings_hours", {}, "")),
          next=("How do I connect WhatsApp?", "How do I create a special?", "What's happening today?")),

    Topic("today", ("today", "bookings today", "dashboard", "overdue", "waiting", "how many bookings", "how busy", "appointment"),
          _today,
          steps=("Today's bookings: every car booked in for today, with buttons to mark it ready, record work, or mark it missed.",
                 "Booking confirmations: requests to confirm or decline.",
                 "Attention: things worth checking now, such as new customers or unanswered chats."),
          links=(("Open the Dashboard", "workshop_dashboard.workshop_dashboard", {}, ""),),
          next=("How do I confirm a booking?", "How do I mark a car as ready?", "How do I add a booking?")),

    Topic("manual_booking", ("add a booking", "new booking", "manual booking", "create a booking", "make a booking", "book a customer",
                             "book someone", "book in a car", "walk in", "walk-in", "phone booking"),
          "You can book a customer in yourself, for example someone who phoned or walked in. The booking is saved as confirmed straight away.",
          steps=("Go to Customers & Vehicles and press \"+ Manual booking\".",
                 "Enter the customer's name and WhatsApp number. If they're already a customer, the booking is added to their record.",
                 "Enter the vehicle (make, model, and registration if you have it).",
                 "Pick the date and type what work needs to be done, then press Save booking. The car is booked in for the morning, when you open."),
          links=(("+ Manual booking", "customer.manual_booking", {}, ""),),
          next=("How do I confirm a booking?", "How do I find a customer?", "What's happening today?")),

    Topic("confirm_booking", ("confirm a booking", "confirm booking", "booking confirmation", "booking request", "pending booking",
                              "accept a booking", "accept booking", "decline a booking", "decline booking", "approve booking"),
          "When a customer books on your booking page, PHANTA asks them on WhatsApp to reply YES to confirm. "
          "Requests you still need to decide on appear on the Dashboard under \"Booking confirmations\".",
          steps=("Open the Dashboard and find the request under \"Booking confirmations\".",
                 "Press Confirm to accept it or Decline to cancel it.",
                 "If WhatsApp can't send the confirmation message (for example, the customer hasn't messaged you in the last 24 hours), "
                 "the booking is still confirmed and PHANTA tells you to contact the customer yourself."),
          links=(("Open the Dashboard", "workshop_dashboard.workshop_dashboard", {}, ""),),
          next=("How do I add a booking?", "Why didn't my customer get a message?")),

    Topic("ready_collected", ("ready for collection", "mark ready", "mark a car as ready", "car is ready", "vehicle ready", "ready for pickup",
                              "collected", "collection", "pick up", "pickup", "notify customer", "mark as collected", "close the job",
                              "unfinished", "work completed", "work outstanding", "missed", "no show"),
          "Each car on today's list moves through a few simple steps, all from the Dashboard:",
          steps=("Press \"Mark ready for pickup\" when the work is done.",
                 "Press \"Notify customer: ready\" to send them a WhatsApp saying their car is ready.",
                 "When they fetch it, press \"Vehicle collected\". This closes the job, so it stops showing as waiting or overdue.",
                 "If the customer didn't arrive, press \"Mark as missed\" and PHANTA sends them a message to rebook.",
                 "Use \"Record work completed\" or \"Work still outstanding\" to note how the job went."),
          links=(("Open the Dashboard", "workshop_dashboard.workshop_dashboard", {}, ""),),
          next=("What's happening today?", "How does the Inbox work?")),

    Topic("find_customer", ("find a customer", "search for a customer", "look up a customer", "find a client", "look up a client",
                            "search customer", "customer details", "find a car", "registration", "find a vehicle"),
          "Use the search box at the top of the dashboard, or open Customers & Vehicles from the sidebar. Both search by name, "
          "WhatsApp number, or vehicle details such as the registration.",
          steps=("Open Customers & Vehicles.", "Type a name, number or registration and press Search.",
                 "Open the customer to see their vehicles, bookings and notes."),
          links=(("Customers & Vehicles", "customer.customers", {}, ""),),
          next=("How do I find a previous booking?", "How do I add a booking?")),

    Topic("booking_history", ("find a booking", "previous booking", "past booking", "old booking", "booking history", "last service", "service history"),
          "Open Customers & Vehicles, find the customer, and open their profile. Their full booking history is listed there, including past and completed jobs.",
          links=(("Customers & Vehicles", "customer.customers", {}, ""),),
          next=("How do I find a customer?", "How do I add a booking?")),

    Topic("inbox", ("inbox", "unanswered", "reply to a customer", "reply to customer", "answer a customer", "chats", "chat", "customer messages",
                    "hand back to ai", "hand back", "ai paused", "talk to the customer myself", "messages"),
          _inbox,
          steps=("Open Inbox and pick a chat on the left.",
                 "Type your reply and press Send. Replying pauses the AI for that customer, so you're in charge of that chat.",
                 "Press \"Hand back to AI\" when you want the assistant to take over again.",
                 "WhatsApp only allows free replies within 24 hours of the customer's last message. After that, the reply box is locked."),
          links=(("Open Inbox", "inbox.inbox", {}, ""),),
          next=("What does the AI assistant do?", "Why didn't my customer get a message?")),

    Topic("ai_assistant", ("ai assistant", "the ai", "ai reply", "ai replies", "bot", "automatic reply", "automatic replies", "auto reply",
                           "who answers", "does the ai", "artificial intelligence"),
          "The AI assistant answers your customers on WhatsApp 24/7. It can answer common questions, collect their car details and book them in "
          "for a morning drop-off on a day you're open. It always says it's an AI. If it can't help, or a customer asks for a person, "
          "it hands the chat to your team in the Inbox.",
          steps=("To take over a chat, reply in the Inbox. The AI pauses for that customer.",
                 "Press \"Hand back to AI\" to let it continue."),
          links=(("Open Inbox", "inbox.inbox", {}, ""),),
          next=("How does the Inbox work?", "How do I connect WhatsApp?")),

    Topic("whatsapp", ("whatsapp", "connect whatsapp", "whatsapp number", "reconnect", "business app", "coexistence", "keep my number", "meta", "phone number", "embedded signup"),
          _whatsapp,
          steps=("Go to Settings → WhatsApp and press Connect WhatsApp (allow pop-ups if nothing opens).",
                 "Sign in with the Facebook account that owns your business, and pick or create your business account.",
                 "Already use the WhatsApp Business app? Choose \"Connect your existing WhatsApp Business app\" and scan the QR code. "
                 "You keep using the app on your phone, and the same chats show in PHANTA's Inbox.",
                 "Otherwise add a new number and enter the code Meta sends you. A new number works in PHANTA's Inbox only, not in a WhatsApp app.",
                 "Add a card in Meta's Billing & payments for the few messages Meta charges for."),
          links=(("Settings → WhatsApp", "settings.settings_whatsapp", {}, ""), ("WhatsApp setup guide", *META, "part-1-connect-whatsapp")),
          next=("Why didn't my customer get a message?", "How does the Inbox work?")),

    Topic("message_not_sent", ("didn't get", "did not get", "didnt get", "get a message", "get the message", "get my message", "got no message", "never got", "not receive", "not received", "message not sent", "not delivered",
                               "template", "24 hour", "24-hour", "message failed", "no message"),
          "WhatsApp has strict rules. You can message a customer freely only within 24 hours of them messaging you. Outside that window, "
          "PHANTA must use a message template approved by Meta, such as booking confirmations, reminders and \"vehicle ready\".",
          steps=("If the customer messaged you recently, check the Inbox. A failed message is marked \"not delivered\".",
                 "If it's been more than 24 hours, the message needs an approved template. PHANTA submits these for you, and Meta usually approves them within hours.",
                 "Make sure a card is added in Meta's Billing & payments. Without it, template messages can't be sent.",
                 "If it keeps failing, ask the customer to send you a WhatsApp first. That opens the 24-hour window."),
          links=(("Open Inbox", "inbox.inbox", {}, ""), ("Add a card for WhatsApp", *META, "add-a-card-for-whatsapp-messages")),
          next=("How do I connect WhatsApp?", "How does the Inbox work?")),

    Topic("flyer_lady", ("flyer lady", "create a special", "new special", "special", "specials", "promotion", "promo", "post", "posting",
                         "publish", "social media", "queue"),
          "Flyer Lady posts your specials for you. Write it once and it goes to Facebook (feed and story), Instagram, and as a WhatsApp Status picture to share. "
          "Every post ends with a \"Book here\" link, and bookings from that link are counted on your Results page.",
          steps=("Open Flyer Lady, write your special (keep the first line short and punchy) and add a picture.",
                 "Press Create special, then Approve.",
                 "Under Post for free, tick where it should go and press Post selected. It's posted within about 5 minutes.",
                 "Optional: under Paid promotion, run it as a Facebook ad and/or a Google search ad with your own budget."),
          links=(("Open Flyer Lady", "flyer_lady.ui", {}, ""),),
          next=("How do I connect Facebook?", "How do I boost a post?", "What are my results?")),

    Topic("facebook", ("facebook", "connect facebook", "facebook page", "page", "fb"),
          "To post to Facebook, connect your workshop's Facebook Page once. You must be an admin of the Page. "
          "If an agency runs your Page, they can give you access without losing theirs.",
          steps=("Open Flyer Lady → Connections → Facebook → Connect.",
                 "Sign in to Facebook and, when asked which Pages to allow, tick your workshop's Page.",
                 "Allow the permissions, then choose your Page in PHANTA. Facebook shows Connected."),
          links=(("Open Flyer Lady", "flyer_lady.ui", {}, ""), ("Facebook setup guide", *META, "part-2-connect-your-facebook-page-posting")),
          next=("How do I create a special?", "How do I boost a post?", "How do I connect Instagram?")),

    Topic("disconnect", ("disconnect", "reconnect", "unlink", "remove facebook", "remove google", "change page", "different page",
                         "change ad account", "switch account", "log out of facebook"),
          "You can disconnect Facebook posting, Facebook ads or Google Ads at any time, for example to pick a different Page or ad account. "
          "Ads that are already running keep running until they end.",
          steps=("Open Flyer Lady → Connections and press Disconnect next to Facebook, Facebook ads or Google Ads.",
                 "Press Connect on the same row to connect again and choose the Page or account you want.",
                 "To also remove PHANTA on Facebook's side: Facebook → Settings & privacy → Settings → Business integrations → remove VANTA Automations FL."),
          links=(("Open Flyer Lady", "flyer_lady.ui", {}, ""),),
          next=("How do I connect Facebook?", "How do I run a Google ad?")),

    Topic("instagram", ("instagram", "insta", "ig"),
          "Instagram connects separately from Facebook. Your Instagram must be a professional account (Business or Creator). "
          "Note that links in Instagram captions can't be tapped, so Instagram brings awareness more than booking clicks.",
          steps=("In the Instagram app: Settings → Account type and tools → Switch to professional account.",
                 "In PHANTA: Flyer Lady → Connections → Instagram → Connect.",
                 "Sign in to Instagram and press Allow."),
          links=(("Open Flyer Lady", "flyer_lady.ui", {}, ""), ("Instagram setup guide", *META, "part-3-connect-instagram")),
          next=("How do I create a special?", "How do I connect Facebook?")),

    Topic("boost", ("boost", "boosted", "paid ad", "paid ads", "advertise", "advertising", "facebook ads", "facebook ad", "meta ads",
                    "ad account", "promote my post", "sponsored", "paid promotion", "promotion", "split", "budget"),
          "Paid promotion shows a special to more people around your workshop as a Facebook ad, a Google search ad, or both. "
          "You choose the total amount, 1 week or 1 month, and how to split it. Meta and Google charge your own cards, never more than your amount.",
          steps=("One-time setup: connect a ZAR Meta ad account (Flyer Lady → Facebook ads) and/or Google Ads, each with your card.",
                 "For a Facebook ad, post the special to your Facebook feed first (Post for free).",
                 "Under the special's Paid promotion, tick Facebook ad and/or Google search ad.",
                 "Enter your total amount, choose 1 week or 1 month, your town and radius, and if you ticked both, slide the split.",
                 "Press Start paid promotion and confirm. Meta and Google review new ads, usually within a day.",
                 "Press Results on each ad to see what's been spent, how many people saw it, and the clicks. PHANTA pauses the Google ad once it has spent its share."),
          links=(("Open Flyer Lady", "flyer_lady.ui", {}, ""), ("Paid boosts guide", *META, "part-4-paid-boosts-adding-your-card")),
          next=("Where do I add my card?", "How do I run a Google ad?", "What are my results?")),

    Topic("card", ("add my card", "add a card", "add card", "card", "credit card", "debit card", "payment method", "pay for ads", "who pays"),
          "Ads and some WhatsApp messages are paid from your own card, directly to Meta or Google. VANTA never handles that money. "
          "Where you add the card depends on what it's for:",
          steps=("Facebook boosts: business.facebook.com → Billing & payments → your ad account → Add payment method.",
                 "WhatsApp messages: business.facebook.com → Billing & payments (or WhatsApp Manager → Payment settings) → your WhatsApp account → Add payment method.",
                 "Google Search ads: in Google Ads → Billing → Settings → add your card (choose automatic payments).",
                 "Your PHANTA subscription itself is on the Billing page in PHANTA."),
          links=(("Meta: add a card for boosts", *META, "step-2-add-your-card"), ("Google: add a card", *GOOGLE, "step-2-add-your-card"),
                 ("PHANTA billing", "billing_statement.billing_statement", {}, "")),
          next=("How do I boost a post?", "How do I run a Google ad?")),

    Topic("google", ("google", "google ads", "google ad", "search ad", "google search", "adwords"),
          "PHANTA can run your special as a Google Search ad, the \"Sponsored\" result people see when they search things like "
          "\"car service Pretoria\" or \"brake repairs\". It's paid from your own Google Ads account, and Google charges per click.",
          steps=("One-time setup: create a Google Ads account in ZAR (Johannesburg time zone) and add your card.",
                 "Connect it: Flyer Lady → Connections → Google Ads → Connect Google Ads, and pick your account.",
                 "Approve a special, then under Paid promotion tick Google search ad, enter your amount, 1 week or 1 month, town and radius, and press Start paid promotion.",
                 "Google reviews the ad, usually within a working day. Clicks and spend show under Results."),
          links=(("Open Flyer Lady", "flyer_lady.ui", {}, ""), ("Google Ads setup guide", *GOOGLE, "")),
          next=("Where do I add my card?", "What are my results?", "How do I boost a post?")),

    Topic("results", ("results", "report", "what did phanta bring", "how many leads", "leads", "clicks", "roi", "worth it",
                      "brought in", "performance", "stats", "statistics"),
          "The Results page shows what PHANTA brought your workshop over the last 7, 30 or 90 days: bookings by where they came from "
          "(Flyer Lady posts, your booking page, the WhatsApp AI), new WhatsApp chats, link clicks, boost budgets, "
          "and a table showing clicks and bookings for each special.",
          steps=("Open Results in the sidebar (owners and managers only).", "Pick 7, 30 or 90 days at the top.",
                 "For a boost's or Google ad's actual spend, press Results on that ad in Flyer Lady."),
          links=(("Open Results", "results.results", {}, ""),),
          next=("How do I create a special?", "How do I boost a post?")),

    Topic("booking_page", ("booking link", "booking page", "online booking", "book online", "website booking", "link to book"),
          "Your workshop has its own online booking page that customers can use without logging in. Every Flyer Lady post links to it, "
          "and you can share the link anywhere: your website, WhatsApp status, or business cards. Customers pick a date and describe the work. "
          "PHANTA then asks them on WhatsApp to confirm.",
          steps=("Open Flyer Lady and press \"Booking page\" on any special to see and copy the link.",),
          links=(("Open Flyer Lady", "flyer_lady.ui", {}, ""),),
          next=("How do I confirm a booking?", "What are my results?")),

    Topic("billing", ("billing", "subscription", "invoice", "how much", "cost", "price of phanta", "pricing", "my account balance", "pay phanta"),
          _billing,
          links=(("Open Billing", "billing_statement.billing_statement", {}, ""),),
          next=("Where do I add my card?",)),

    Topic("settings", ("opening hours", "operating hours", "business hours", "trading hours", "closed on", "open on", "settings",
                       "add a user", "add staff", "users", "staff", "team member", "password", "change password", "notifications", "business details"),
          "Most workshop settings live under Settings in the sidebar:",
          steps=("Opening hours: which days and times you're open. Bookings only land on open days.",
                 "Users & permissions: add staff and choose what they can see.",
                 "Business details: your name, address and contact number.",
                 "Notifications: which WhatsApp messages customers receive.",
                 "Password: change your own password."),
          links=(("Opening hours", "settings.settings_hours", {}, ""), ("Users", "settings.settings_users", {}, ""), ("All settings", "settings.settings_overview", {}, "")),
          next=("How do I connect WhatsApp?", "How do I add a booking?")),

    Topic("privacy", ("privacy", "private", "who can see", "is my data safe", "data safe", "confidential", "popia", "my data"),
          "Your workshop's customer and booking information is kept separate from every other workshop using PHANTA. Only your own team can see it, "
          "and staff only see what their role allows (set in Settings → Users).",
          links=(("Users & permissions", "settings.settings_users", {}, ""),),
          next=("How do I add a user?",)),

    Topic("service_advisor", ("service advisor", "maintenance", "service due", "next service", "service reminder"),
          "The Service Advisor keeps track of when each vehicle is due for its next service, based on standard service intervals. "
          "PHANTA can then remind customers on WhatsApp when their car is due. It never sets your prices or approves repairs: those stay your decisions.",
          links=(("Customers & Vehicles", "customer.customers", {}, ""),),
          next=("How do I find a customer?", "Why didn't my customer get a message?")),
)

FALLBACK = Topic(
    "fallback", (),
    "I'm not sure I understood that one, sorry. 🙈 I can walk you through anything in PHANTA, or tell you today's numbers. Try one of these, or ask in your own words:",
    next=("What's happening today?", "How do I add a booking?", "How do I connect WhatsApp?", "How do I boost a post?", "What are my results?"),
)


def normalize(text: str) -> str:
    return " ".join(re.sub(r"[^a-z0-9]+", " ", (text or "").lower().replace("'", "")).split())


def match(text: str) -> Topic:
    """Best topic for the question: the longest matching phrase wins; ties go to the earlier topic."""
    text = normalize(text)
    padded = f" {text} "
    best, best_score = FALLBACK, 0
    for topic in TOPICS:
        phrases = [normalize(p) for p in topic.phrases]
        # Short phrases (hi, ig, fb, page) must match as whole words, so "this" doesn't mean "hi".
        score = max((len(p) for p in phrases if (f" {p} " in padded if len(p) <= 4 else p in text)), default=0)
        if score > best_score:
            best, best_score = topic, score
    return best
