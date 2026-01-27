# 🦅 Kite Mobile Dashboard

This is the standalone web version of the OMRE Intelligence Dashboard, designed for mobile performance and easy cloud deployment.

## 🚀 How to Run Locally

1. **Enter Folder**:
   ```bash
   cd KITE_WEBSITE
   ```

2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Setup Environment**:
   Create a `.env` file inside this folder with:
   ```env
   KITE_API_KEY=your_key
   KITE_API_SECRET=your_secret
   CLOUD_DATABASE_URL=postgresql://...
   ```

4. **Start Server**:
   ```bash
   python main.py
   ```
   Visit `http://localhost:5000` on your PC, or `http://[YOUR-IP]:5000` on your Phone.

---

## ☁️ How to Deploy to Cloud (Render)

1. **Create a Private GitHub Repo** and upload only the contents of this `KITE_WEBSITE` folder.
2. **Go to [Render.com](https://render.com/)** and create a new **Web Service**.
3. **Connect your Repo**.
4. **Settings**:
   - **Runtime**: `Python`
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `gunicorn main:app`
5. **Add Environment Variables** in Render Dashboard (matching your `.env`).
6. **Done!** Your website will be live at `https://your-app.onrender.com`.

---

## 📱 Mobile features
- **Glassmorphism UI**: High-end premium design.
- **Real-time Sync**: Pulls latest scores from the Cloud DB.
- **OAuth Login**: Regenerate Kite tokens directly from your phone.
