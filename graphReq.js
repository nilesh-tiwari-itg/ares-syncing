export const shopifyGraphQLRequest = async (query, variables = {}) => {
  try {
    const myHeaders = new Headers();
    myHeaders.append("Content-Type", "application/json");
    myHeaders.append("X-Shopify-Access-Token", process.env.SHOPIFY_ACCESS_TOKEN);
 
    const body = JSON.stringify({
      query,
      variables,
    });
 
    const requestOptions = {
      method: "POST",
      headers: myHeaders,
      body,
      redirect: "follow",
    };
 
    const response = await fetch(
      `https://${process.env.SHOPIFY_SHOP}/admin/api/2025-10/graphql.json`,
      requestOptions
    );
 
    const responseData = await response.json();
 
    if (responseData.errors) {
      console.error("Shopify GraphQL Errors:", JSON.stringify(responseData.errors, null, 2));
      return false;
    }
 
    return responseData.data;
  } catch (error) {
    console.error("Catch error in shopifyGraphQLRequest:", JSON.stringify(error, null, 2));
    return false;
  }
};