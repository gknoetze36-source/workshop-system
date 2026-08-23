"""Industry-owned automation workflow definitions.

The execution engine is universal. These definitions are the industry layer.
A Location selects its industry; only that industry's workflows are exposed.
"""
from dataclasses import dataclass

@dataclass(frozen=True)
class WorkflowDefinition:
    industry: str
    name: str
    event_type: str
    trigger_timing: str
    default_delay_minutes: int
    default_message: str

WORKFLOW_DEFINITIONS = (
    WorkflowDefinition('workshop', 'Booking confirmation', 'booking.created', 'immediate', 0, 'Your booking is confirmed. We will see you at {business_name}.'),
    WorkflowDefinition('workshop', 'Missed booking recovery', 'booking.missed', 'same_day', 60, 'We missed you today. Would you like us to help book a new time?'),
    WorkflowDefinition('workshop', 'Yearly service reminder', 'service.annual_due', 'annual', 0, 'Your yearly service reminder is due. Would you like us to book you in?'),
    WorkflowDefinition('workshop', 'Appointment Reminder', 'booking.reminder_due', 'day_before', 1440, 'Reminder: Your appointment for {make} {model} is scheduled for {scheduled_date}. Please let us know if you need to reschedule.'),
    WorkflowDefinition('workshop', 'Vehicle Ready Notification', 'vehicle.ready_for_pickup', 'immediate', 0, 'Great news! Your {make} {model} is ready for collection. Please come collect it at your convenience.'),
    WorkflowDefinition('workshop', 'Work in Progress Update', 'work.in_progress', 'after_delay', 240, "Just updating you on your {make} {model} service - work is progressing well. We'll let you know when it's ready for collection."),
    WorkflowDefinition('workshop', 'Service Reminder', 'service.reminder_due', 'after_delay', 43200, "It's time for your vehicle's regular service. Would you like to book an appointment?"),
    WorkflowDefinition('workshop', 'Thank You Message', 'booking.completed', 'immediate', 0, "Thank you for choosing our workshop! We hope you're satisfied with the service on your {make} {model}. Don't forget to schedule your next maintenance visit."),
    WorkflowDefinition('salon', 'Booking confirmation', 'booking.created', 'immediate', 0, 'Your appointment is confirmed with {business_name}.'),
    WorkflowDefinition('salon', 'Appointment Reminder', 'booking.reminder_due', 'day_before', 1440, 'Reminder: Your appointment for {service} is scheduled for {scheduled_date}. Please let us know if you need to reschedule.'),
    WorkflowDefinition('salon', 'Service Reminder', 'service.reminder_due', 'after_delay', 2592000, "It's time for your regular {service} appointment. Would you like to book your next visit?"),
    WorkflowDefinition('salon', 'Thank You Message', 'booking.completed', 'immediate', 0, 'Thank you for visiting our salon! We hope you enjoyed your {service} service. We look forward to seeing you again soon.'),
    WorkflowDefinition('dentist', 'Appointment reminder', 'booking.reminder_due', 'day_before', 1440, 'Reminder: your dental appointment is coming up with {business_name}.'),
    WorkflowDefinition('dentist', 'Appointment Reminder', 'booking.reminder_due', 'day_before', 1440, 'Reminder: Your dental appointment is scheduled for {scheduled_date}. Please let us know if you need to reschedule.'),
    WorkflowDefinition('dentist', 'Checkup reminder', 'service.recurring_due', 'six_monthly', 0, 'It is time for your dental checkup. Would you like to book?'),
    WorkflowDefinition('dentist', 'Service Reminder', 'service.reminder_due', 'after_delay', 2592000, "It's time for your regular dental checkup. Would you like to schedule your next appointment?"),
    WorkflowDefinition('dentist', 'Thank You Message', 'booking.completed', 'immediate', 0, "Thank you for visiting our dental clinic! We hope you're pleased with your treatment. Remember to schedule your next check-up for optimal oral health."),
    WorkflowDefinition('clinic', 'Appointment reminder', 'booking.reminder_due', 'day_before', 1440, 'Reminder: your appointment is coming up with {business_name}.'),
    WorkflowDefinition('clinic', 'Appointment Reminder', 'booking.reminder_due', 'day_before', 1440, 'Reminder: Your appointment is scheduled for {scheduled_date}. Please let us know if you need to reschedule.'),
    WorkflowDefinition('clinic', 'Service Reminder', 'service.reminder_due', 'after_delay', 2592000, "It's time for your regular check-up. Would you like to schedule your next visit?"),
    WorkflowDefinition('clinic', 'Thank You Message', 'booking.completed', 'immediate', 0, 'Thank you for visiting our clinic! We hope you had a positive experience. We look forward to seeing you again for your next appointment.'),
    WorkflowDefinition('hotel', 'Check-in reminder', 'booking.reminder_due', 'day_before', 1440, 'Your check-in at {business_name} is coming up. Reply if you need help.'),
    WorkflowDefinition('hotel', 'Appointment Reminder', 'booking.reminder_due', 'day_before', 1440, 'Reminder: Your reservation is scheduled for {scheduled_date}. Please let us know if you need to make any changes.'),
    WorkflowDefinition('hotel', 'Service Reminder', 'service.reminder_due', 'after_delay', 2592000, "We hope you're enjoying your stay! Would you like to extend your booking or book any additional services?"),
    WorkflowDefinition('hotel', 'Thank You Message', 'booking.completed', 'immediate', 0, 'Thank you for staying with us! We hope you had a wonderful experience. We look forward to welcoming you back soon.'),
    WorkflowDefinition('consultant', 'Lead follow-up', 'inquiry.created', 'after_delay', 30, 'Thanks for reaching out. Would you like me to secure a consultation time?'),
    WorkflowDefinition('consultant', 'Appointment Reminder', 'booking.reminder_due', 'day_before', 1440, 'Reminder: Your consultation is scheduled for {scheduled_date}. Please let us know if you need to reschedule.'),
    WorkflowDefinition('consultant', 'Thank You Message', 'booking.completed', 'immediate', 0, "Thank you for consulting with us! We hope you found our advice valuable. Please don't hesitate to reach out if you have any follow-up questions."),
    WorkflowDefinition('gym', 'Class reminder', 'booking.reminder_due', 'same_day', 120, 'Reminder: your class/session at {business_name} is coming up.'),
    WorkflowDefinition('gym', 'Appointment Reminder', 'booking.reminder_due', 'day_before', 1440, 'Reminder: Your fitness session is scheduled for {scheduled_date}. Please let us know if you need to reschedule.'),
    WorkflowDefinition('gym', 'Service Reminder', 'service.reminder_due', 'after_delay', 2592000, "It's time for your fitness check-in or to renew your membership. Would you like to schedule your next session?"),
    WorkflowDefinition('gym', 'Thank You Message', 'booking.completed', 'immediate', 0, 'Thank you for working out with us! We hope you enjoyed your session. Remember to stay hydrated and keep up the great work!'),
    WorkflowDefinition('cleaning', 'Job confirmation', 'booking.created', 'immediate', 0, 'Your cleaning booking is confirmed with {business_name}.'),
    WorkflowDefinition('cleaning', 'Appointment Reminder', 'booking.reminder_due', 'day_before', 1440, 'Reminder: Your cleaning service is scheduled for {scheduled_date}. Please let us know if you need to reschedule.'),
    WorkflowDefinition('cleaning', 'Thank You Message', 'booking.completed', 'immediate', 0, "Thank you for choosing our cleaning service! We hope you're satisfied with the results. We look forward to helping you keep your space clean and tidy."),
    WorkflowDefinition('repair', 'Quote follow-up', 'quote.pending', 'after_delay', 120, 'Just following up on your quote. Would you like us to proceed?'),
    WorkflowDefinition('repair', 'Appointment Reminder', 'booking.reminder_due', 'day_before', 1440, 'Reminder: Your repair service is scheduled for {scheduled_date}. Please let us know if you need to reschedule.'),
    WorkflowDefinition('repair', 'Vehicle Ready Notification', 'vehicle.ready_for_pickup', 'immediate', 0, 'Great news! Your {make} {model} is ready for collection. Please come collect it at your convenience.'),
    WorkflowDefinition('repair', 'Work in Progress Update', 'work.in_progress', 'after_delay', 240, "Just updating you on your {make} {model} repair - work is progressing well. We'll let you know when it's ready for collection."),
    WorkflowDefinition('repair', 'Thank You Message', 'booking.completed', 'immediate', 0, "Thank you for choosing our repair service! We hope you're satisfied with the work done on your {make} {model}. We stand behind our work with a satisfaction guarantee."),
)

def workflows_for_industry(industry: str):
    return tuple(w for w in WORKFLOW_DEFINITIONS if w.industry == industry)
