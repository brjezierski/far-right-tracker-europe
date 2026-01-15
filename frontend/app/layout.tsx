import "./globals.css";

export const metadata = {
  title: "Suport for Far-Right and National Conservative Parties",
  description: "Choropleth of nationalist party support across Europe",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
