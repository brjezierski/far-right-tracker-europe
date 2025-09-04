/** @type {import('next').NextConfig} */
const nextConfig = {
  output: "standalone",
  reactStrictMode: true,
  experimental: {
    esmExternals: "loose",
  },
};

module.exports = nextConfig;
