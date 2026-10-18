// SSLCOMMERZ Payment Gateway Integration for Dispo Smart Vending
// Supports bKash, Nagad, Rocket, Upay, Cards (Visa, Mastercard), & Net Banking

const STORE_ID = process.env.SSLCOMMERZ_STORE_ID || "testbox";
const STORE_PASSWD = process.env.SSLCOMMERZ_STORE_PASSWD || "qwerty";
const IS_LIVE = process.env.SSLCOMMERZ_IS_LIVE === "true";

const BASE_URL = IS_LIVE
  ? "https://securepay.sslcommerz.com"
  : "https://sandbox.sslcommerz.com";

export async function initSSLCommerzPayment({
  orderId,
  amount,
  customerName = "Dispo Customer",
  customerEmail = "customer@dispo.app",
  customerPhone = "01700000000",
  serverUrl = process.env.SERVER_URL || "https://vending-server.vercel.app",
}) {
  const params = new URLSearchParams({
    store_id: STORE_ID,
    store_passwd: STORE_PASSWD,
    total_amount: String(amount),
    currency: "BDT",
    tran_id: String(orderId),
    success_url: `${serverUrl}/api/payments/sslcommerz/success`,
    fail_url: `${serverUrl}/api/payments/sslcommerz/fail`,
    cancel_url: `${serverUrl}/api/payments/sslcommerz/cancel`,
    ipn_url: `${serverUrl}/api/payments/sslcommerz/ipn`,
    cus_name: customerName,
    cus_email: customerEmail,
    cus_phone: customerPhone,
    cus_add1: "Dhaka",
    cus_city: "Dhaka",
    cus_country: "Bangladesh",
    shipping_method: "NO",
    product_name: `Dispo Order #${String(orderId).slice(-6)}`,
    product_category: "Vending",
    product_profile: "general",
  });

  const response = await fetch(`${BASE_URL}/gwprocess/v4/api.php`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: params.toString(),
  });

  const data = await response.json();

  if (data.status === "SUCCESS" && data.GatewayPageURL) {
    return {
      success: true,
      gatewayUrl: data.GatewayPageURL,
      sessionkey: data.sessionkey,
    };
  }

  throw new Error(data.failedreason || "Failed to initialize SSLCOMMERZ payment");
}

export async function validateSSLCommerzPayment(valId) {
  if (!valId) return { isValid: false, error: "Missing val_id" };

  const url = `${BASE_URL}/validator/api/validationserverAPI.php?val_id=${encodeURIComponent(
    valId
  )}&store_id=${encodeURIComponent(STORE_ID)}&store_passwd=${encodeURIComponent(
    STORE_PASSWD
  )}&format=json`;

  const response = await fetch(url);
  const data = await response.json();

  // status is "VALID" or "VALIDATED" when payment actually cleared
  const isValid = data.status === "VALID" || data.status === "VALIDATED";
  return { isValid, data };
}
