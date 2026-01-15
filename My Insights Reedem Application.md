
# Ideas Around FIFO Logic

The current solution that I built was designed based on the project specifications. However, I think additional logic would need to be implemented to make this a useful and accurate solution.  

The additional field of `REDEEMID` allows for the oldest "earned" transaction to be identified, but it does not specify a credit amount that should be applied to the "spent" amount. This could lead to leftover open balances on both the spent and earned sides of the transactions.

---

## Potential Solution: Ledger Apply Table

A potential solution would be to implement a **ledger apply table**, where spent transactions could be applied to multiple earned transactions with corresponding amounts for each credit application.

A simplified table schema could look like this:
TS_LEDGER_APPLY

| Spend_TranID | Earned_TranID | Amount_Applied | Date_Applied | Program_User_Stamp |
|--------------|---------------|----------------|--------------|------------------|
| 12011454     | 11561486      | 0.94           | 01/14/2025   | BatchApplicationTask |
| 12011454     | 11561487      | 0.90           | 01/14/2025   | BatchApplicationTask |
| 12011454     | 11127421      | 14.23          | 01/14/2025   | BatchApplicationTask |

The original `TC_Data` table could then hold the original amounts, with an additional column called `OpenAmount`, which reflects the total amount of credits that have been applied to the transaction.

---

## Example Final Table

Assuming the three earned credits were applied, the final table could reflect that all credits were used toward that transaction, with no remaining outstanding amounts:

| TRANS_ID  | TCTYPE  | CREATEDAT           | EXPIREDAT           | CUSTOMERID | ORDERID  | AMOUNT  | REASON                   | OPENAMOUNT |
|-----------|--------|-------------------|-------------------|------------|----------|--------|-------------------------|------------|
| 12011454  | spent  | 2023-04-05 16:59:39 |                   | 16161481   | 39061923 | -16.07 |                         | 0          |
| 11561486  | earned | 2023-03-08 23:06:16 | 2023-04-07 23:06:16 | 16161481   |          | 0.94   | Refund                  | 0          |
| 11561487  | earned | 2023-03-08 23:06:16 | 2023-04-07 23:06:16 | 16161481   |          | 0.90   | Refund                  | 0          |
| 11127421  | earned | 2023-02-05 13:26:14 | 2023-04-07 13:26:14 | 16161481   |          | 14.23  | Delivered, Not Received | 0          |

---

## Additional Considerations

There are several other factors and to-do items to consider:

- Error handling and pipeline monitoring.
- Alerts and reporting for record counts of updated records.
- Reconciliation of amounts applied.
- Tie-outs of totals across raw, staging, and processed tables/schemas.

> **Note:** Time constraints and the use of Snowflake Tasks instead of dbt/Airflow limited implementation options. These ideas, however, would improve usability for accounting teams working with this data.

---



