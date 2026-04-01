export async function onRequestGet(context) {
  return new Response(
    JSON.stringify({
      supabaseUrl: context.env.SUPABASE_URL || "",
      supabaseAnonKey: context.env.SUPABASE_ANON_KEY || "",
    }),
    {
      headers: {
        "Content-Type": "application/json",
        "Cache-Control": "no-store",
      },
    }
  );
}
