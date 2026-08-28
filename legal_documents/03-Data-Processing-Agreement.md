# Data Processing Agreement (POPIA Operator Agreement)

This Data Processing Agreement ("DPA") is entered into between **[Company Legal Name]** ("PHANTA", "Operator") and the Customer identified in the applicable PHANTA Account ("Customer", "Responsible Party"), and forms part of the Terms of Service. It is entered into pursuant to **section 21 of the Protection of Personal Information Act 4 of 2013 ("POPIA")**, which requires a written contract wherever an Operator processes personal information on a Responsible Party's behalf.

## 1. Roles of the parties
For personal information of Customer's own end customers (e.g. vehicle owners, their contact details, and service/booking history) processed through the Service, **Customer is the Responsible Party** and **PHANTA is the Operator**. This DPA does not apply to Account-holder data (Customer's own staff details, billing information), for which PHANTA is Responsible Party in its own right, as described in the Privacy Policy.

## 2. Subject matter, duration, nature, and purpose of processing
- **Subject matter:** provision of the PHANTA workshop automation platform, including automated WhatsApp messaging, booking, quotation, and billing features.
- **Duration:** for the term of the Terms of Service, and thereafter only as needed to comply with clause 8 (return/deletion).
- **Nature of processing:** collection, storage, organisation, transmission, and automated processing (including AI-assisted message generation) of personal information via the Service.
- **Purpose:** to enable Customer to manage bookings, communicate with its own customers, generate quotations and invoices, and operate its workshop business through the Service.

## 3. Categories of data subjects and personal information
- **Data subjects:** Customer's end customers (vehicle owners) and, where relevant, their nominated contacts.
- **Categories of personal information:** name, phone number, vehicle identifying details, service and booking history, WhatsApp message content and delivery metadata, marketing consent status and its evidence, and (where Customer chooses to record it) email address.
- **No special personal information** (as defined in POPIA, e.g. health, biometric, or similarly sensitive categories) is intended to be processed through the Service. Customer must not submit special personal information via the Service without PHANTA's prior written agreement on additional safeguards. Customer acknowledges that free-text fields (such as job notes and message content) are not technically restricted, and that responsibility for not entering special personal information into them rests with Customer.

## 4. Operator obligations (POPIA sections 19–21)
PHANTA will:
1. **Process only on documented instructions** from Customer (given via the Service's configuration, this DPA, and the Terms of Service), and not for its own purposes, unless required by South African law — in which case PHANTA will inform Customer of that legal requirement first, unless prohibited from doing so.
2. **Maintain confidentiality**, ensuring personnel authorised to process personal information are subject to a duty of confidentiality.
3. **Implement and maintain security measures** at least equivalent to those required by POPIA section 19, appropriate to the risk. As at the effective date these include:
   - **Tenant isolation enforced at the database layer**, so information belonging to one Customer cannot be read from another Customer's account even in the event of an application-level fault;
   - **Encryption of third-party integration credentials at rest**, and encryption of data in transit over HTTPS/TLS;
   - **Role-based access control**, so that staff accounts within a Customer's Account only reach the functions appropriate to their role;
   - **Authentication and administrative audit logging**, recording who did what, when, to which record, and with what result;
   - **Immediate session termination** on password change, administrative password reset, or account deactivation;
   - **Rate limiting** on authentication and other sensitive endpoints;
   - **Verification of the authenticity of inbound messages** from integrated providers by cryptographic signature;
   - **Automated deletion of WhatsApp message content** 14 days after sending or receipt (see clause 8);
   - **Restriction of information sent to AI sub-operators** to the minimum required for the specific request (see clause 5).
   [Confirm this list against the deployed configuration at the time of publishing, and update it when the configuration changes.]
4. **Notify Customer without undue delay** on becoming aware of reasonable grounds to believe personal information processed under this DPA has been accessed or acquired by any unauthorised person, providing sufficient detail to allow Customer to meet its own section 22 notification duties to the Information Regulator and affected data subjects. PHANTA maintains an internal incident register recording the systems and Customers affected and the records potentially involved, to support accurate notification.
5. **Assist Customer** in responding to data subject requests (access, correction, deletion, objection) relating to information processed under this DPA, to the extent PHANTA is able. The Service provides Customer with self-service export of its Account's data and self-service deletion of an individual end customer's identifying details.
6. **Not retain** personal information for longer than necessary to perform the Service, subject to clause 8.

## 5. Sub-processors ("Sub-Operators")
Customer authorises PHANTA to engage the following sub-operators as at the effective date of this DPA:

| Sub-operator | Purpose | Personal information received | Processing location |
|---|---|---|---|
| Meta / WhatsApp Business Platform | Delivery and receipt of WhatsApp messages | End customer phone number, message content, delivery metadata | [confirm] |
| Paystack | Payment processing for Customer's subscription to PHANTA | Account billing contact and transaction data. No end-customer data. | South Africa |
| OpenAI | AI-assisted message and content generation | Limited to the individual customer's name, their vehicles, their open bookings, and the recent conversation for the request in question | United States [confirm current region] |
| Railway | Application and database hosting | All Service data, as hosting infrastructure | [confirm] |
| Sentry | Application error and diagnostic monitoring | Diagnostic and error data. Personal information is suppressed; request bodies, cookies, and authentication headers are stripped before transmission. | [confirm] |
| Google Business Profile | Business listing and review request management, where the Customer enables it | Customer's own business listing data and review content. No end-customer contact details. | [confirm] |

PHANTA does **not** integrate with Google Calendar, and no booking or personal information is transmitted to a calendar provider.

PHANTA will impose data protection obligations on each sub-operator that are no less protective than this DPA, remains liable to Customer for each sub-operator's performance, and will give Customer reasonable notice of any intended change to this list so Customer may object on reasonable data-protection grounds.

## 6. Cross-border transfers (POPIA section 72)
Where a sub-operator processes personal information outside South Africa, PHANTA will ensure the transfer is subject to a written agreement requiring the recipient to apply protection substantially similar to POPIA's conditions, including onward-transfer restrictions to any further country, before the transfer occurs.

## 7. Audit and demonstration of compliance
On reasonable written notice, and no more than [once per 12 months] (except following a security incident), PHANTA will provide Customer with reasonably requested information to demonstrate compliance with this DPA, which may include a summary of security measures, relevant certifications, or a mutually agreed audit, at Customer's cost, subject to confidentiality.

## 8. Return or deletion on termination
On termination or expiry of the Terms of Service, PHANTA will, at Customer's election, delete or return all personal information processed under this DPA within [30] days, except to the extent retention is required by South African law (e.g. FICA record-keeping for billing data), in which case PHANTA will continue to protect that information under this DPA's security obligations until deletion is permitted.

Customer is given the opportunity to export its data before deletion is carried out.

**How deletion is performed.** Deletion is effected by removing directly identifying information (name, contact number, email address) and de-identifying the remaining service and booking history, so that the records can no longer be attributed to an identifiable person while the Customer's operational and financial history remains intact. Billing records, records of legal acceptance, and security and audit logs are retained where required for legal or accountability purposes, as set out in clause 8 above and in the Privacy Policy.

**Backups.** De-identified information may persist in routine infrastructure backups until those backups age out of the hosting provider's retention cycle, currently approximately [confirm days] days. Backups are not used to restore deleted information except in a disaster-recovery event, after which deletion is re-applied.

## 9. Liability
Liability under this DPA is subject to the limitation of liability clause in the Terms of Service, save that nothing in this DPA limits either party's liability for a breach of the confidentiality or security obligations in clauses 4.2–4.3 to the extent such limitation is not permitted by law.

## 10. General
This DPA is governed by the laws of the Republic of South Africa and takes precedence over the Terms of Service to the extent of any conflict regarding the processing of personal information. It remains in force for as long as PHANTA processes personal information on Customer's behalf.

*[Legal review required — see 00-README before publishing. The sub-operator table above reflects the integrations actually present in the Service as at the date of this revision; re-verify it whenever an integration is added or removed, as an inaccurate sub-operator table is itself a compliance gap.]*
