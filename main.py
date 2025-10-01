from flask import Flask, render_template, request, flash, redirect, url_for, jsonify
from flask import send_from_directory
from datetime import datetime
from groq import Groq
import pandas as pd
import io
import os
import math
from flask import send_file


app = Flask(__name__)
app.secret_key = 'your_secret_key_here'  # Required for flash messages

AI_KEY = os.getenv('GROQ_API_KEY', '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')

client = Groq(api_key=AI_KEY)

# Global variable to store uploaded data
uploaded_data = None

def analyze_business_data(df):
    """Menganalisis data bisnis berdasarkan rating dan jumlah ulasan"""
    try:
        # Pastikan kolom yang diperlukan ada
        required_columns = ['nama', 'rating', 'jumlah_ulasan']
        for col in required_columns:
            if col not in df.columns:
                return None, f"Kolom '{col}' tidak ditemukan dalam file CSV"
        
        # Buat salinan dataframe untuk menghindari SettingWithCopyWarning
        df_clean = df.copy()
        
        # Konversi kolom numerik
        df_clean['rating'] = pd.to_numeric(df_clean['rating'], errors='coerce')
        df_clean['jumlah_ulasan'] = pd.to_numeric(df_clean['jumlah_ulasan'], errors='coerce')
        
        # Hapus baris dengan nilai NaN
        df_clean = df_clean.dropna(subset=['rating', 'jumlah_ulasan'])
        
        if len(df_clean) == 0:
            return None, "Tidak ada data yang valid untuk dianalisis"
        
        # Reset index untuk menghindari masalah indexing
        df_clean = df_clean.reset_index(drop=True)
        
        # Analisis 1: Bisnis dengan rating tertinggi
        if not df_clean.empty and 'rating' in df_clean.columns:
            highest_rated_idx = df_clean['rating'].idxmax()
            highest_rated = df_clean.loc[highest_rated_idx]
        else:
            highest_rated = pd.Series({'nama': 'Tidak ada data', 'rating': 0, 'jumlah_ulasan': 0, 'kategori_usaha': 'Tidak tersedia'})
        
        # Analisis 2: Bisnis dengan jumlah ulasan terbanyak
        if not df_clean.empty and 'jumlah_ulasan' in df_clean.columns:
            most_reviewed_idx = df_clean['jumlah_ulasan'].idxmax()
            most_reviewed = df_clean.loc[most_reviewed_idx]
        else:
            most_reviewed = pd.Series({'nama': 'Tidak ada data', 'rating': 0, 'jumlah_ulasan': 0, 'kategori_usaha': 'Tidak tersedia'})
        
        # Analisis 3: Bisnis dengan rating terendah (minimal 10 ulasan)
        low_rated_filtered = df_clean[df_clean['jumlah_ulasan'] >= 10]
        if len(low_rated_filtered) > 0:
            lowest_rated_idx = low_rated_filtered['rating'].idxmin()
            lowest_rated = low_rated_filtered.loc[lowest_rated_idx]
        elif len(df_clean) > 0:
            lowest_rated_idx = df_clean['rating'].idxmin()
            lowest_rated = df_clean.loc[lowest_rated_idx]
        else:
            lowest_rated = pd.Series({'nama': 'Tidak ada data', 'rating': 0, 'jumlah_ulasan': 0, 'kategori_usaha': 'Tidak tersedia'})
        
        # Analisis 4: Rata-rata rating dan ulasan
        avg_rating = df_clean['rating'].mean() if not df_clean.empty else 0
        avg_reviews = df_clean['jumlah_ulasan'].mean() if not df_clean.empty else 0
        
        # Analisis 5: Kategori usaha unik
        categories = {}
        if 'kategori_usaha' in df_clean.columns and not df_clean.empty:
            categories = df_clean['kategori_usaha'].value_counts().head(10).to_dict()
        
        # Analisis 6: Top 10 bisnis berdasarkan rating
        top_10_rating = []
        if not df_clean.empty:
            top_10_rating_df = df_clean.nlargest(10, 'rating')
            # Pastikan kolom yang diperlukan ada
            columns_to_include = ['nama', 'rating', 'jumlah_ulasan']
            if 'kategori_usaha' in top_10_rating_df.columns:
                columns_to_include.append('kategori_usaha')
            top_10_rating = top_10_rating_df[columns_to_include].to_dict('records')
        
        # Analisis 7: Top 10 bisnis berdasarkan jumlah ulasan
        top_10_reviews = []
        if not df_clean.empty:
            top_10_reviews_df = df_clean.nlargest(10, 'jumlah_ulasan')
            columns_to_include = ['nama', 'rating', 'jumlah_ulasan']
            if 'kategori_usaha' in top_10_reviews_df.columns:
                columns_to_include.append('kategori_usaha')
            top_10_reviews = top_10_reviews_df[columns_to_include].to_dict('records')
        
        # Analisis 8: Distribusi rating
        rating_distribution = {}
        if not df_clean.empty:
            rating_distribution = df_clean['rating'].value_counts().sort_index().to_dict()
        
        # Analisis 9: Statistik lengkap
        stats = {
            'total_businesses': len(df_clean),
            'avg_rating': round(avg_rating, 2) if not pd.isna(avg_rating) else 0,
            'avg_reviews': round(avg_reviews, 2) if not pd.isna(avg_reviews) else 0,
            'max_rating': round(df_clean['rating'].max(), 2) if not df_clean.empty else 0,
            'min_rating': round(df_clean['rating'].min(), 2) if not df_clean.empty else 0,
            'max_reviews': int(df_clean['jumlah_ulasan'].max()) if not df_clean.empty else 0,
            'min_reviews': int(df_clean['jumlah_ulasan'].min()) if not df_clean.empty else 0,
            'total_reviews': int(df_clean['jumlah_ulasan'].sum()) if not df_clean.empty else 0
        }
        
        # Handle missing values in the series
        def get_series_value(series, key, default='Tidak tersedia'):
            try:
                value = series.get(key, default)
                return value if not pd.isna(value) else default
            except:
                return default
        
        results = {
            'highest_rated': {
                'nama': get_series_value(highest_rated, 'nama', 'Tidak ada data'),
                'rating': round(float(get_series_value(highest_rated, 'rating', 0)), 2),
                'jumlah_ulasan': int(get_series_value(highest_rated, 'jumlah_ulasan', 0)),
                'kategori': get_series_value(highest_rated, 'kategori_usaha', 'Tidak tersedia')
            },
            'most_reviewed': {
                'nama': get_series_value(most_reviewed, 'nama', 'Tidak ada data'),
                'rating': round(float(get_series_value(most_reviewed, 'rating', 0)), 2),
                'jumlah_ulasan': int(get_series_value(most_reviewed, 'jumlah_ulasan', 0)),
                'kategori': get_series_value(most_reviewed, 'kategori_usaha', 'Tidak tersedia')
            },
            'lowest_rated': {
                'nama': get_series_value(lowest_rated, 'nama', 'Tidak ada data'),
                'rating': round(float(get_series_value(lowest_rated, 'rating', 0)), 2),
                'jumlah_ulasan': int(get_series_value(lowest_rated, 'jumlah_ulasan', 0)),
                'kategori': get_series_value(lowest_rated, 'kategori_usaha', 'Tidak tersedia')
            },
            'statistics': stats,
            'categories': categories,
            'top_10_rating': top_10_rating,
            'top_10_reviews': top_10_reviews,
            'rating_distribution': rating_distribution,
            'raw_data': df_clean.to_dict('records')  # Semua data bersih
        }
        
        return results, None
        
    except Exception as e:
        import traceback
        print(f"Error in analyze_business_data: {str(e)}")
        print(traceback.format_exc())
        return None, f"Error dalam menganalisis data: {str(e)}"

def ai_call(df, analysis_results):
    """Meminta analisis AI berdasarkan data dan hasil analisis"""
    try:
        # Siapkan ringkasan data untuk AI
        data_summary = f"""
        Data bisnis yang dianalisis:
        - Total bisnis: {analysis_results['statistics']['total_businesses']}
        - Rata-rata rating: {analysis_results['statistics']['avg_rating']}
        - Rata-rata jumlah ulasan: {analysis_results['statistics']['avg_reviews']}
        - Total ulasan: {analysis_results['statistics']['total_reviews']}
        
        Bisnis dengan rating tertinggi: {analysis_results['highest_rated']['nama']} 
        (Rating: {analysis_results['highest_rated']['rating']}, 
        Ulasan: {analysis_results['highest_rated']['jumlah_ulasan']},
        Kategori: {analysis_results['highest_rated']['kategori']})
        
        Bisnis dengan ulasan terbanyak: {analysis_results['most_reviewed']['nama']}
        (Rating: {analysis_results['most_reviewed']['rating']}, 
        Ulasan: {analysis_results['most_reviewed']['jumlah_ulasan']},
        Kategori: {analysis_results['most_reviewed']['kategori']})
        
        Bisnis dengan rating terendah: {analysis_results['lowest_rated']['nama']}
        (Rating: {analysis_results['lowest_rated']['rating']}, 
        Ulasan: {analysis_results['lowest_rated']['jumlah_ulasan']},
        Kategori: {analysis_results['lowest_rated']['kategori']})
        """
        
        chat_completion = client.chat.completions.create(
            messages=[
                {
                    "role": "user", 
                    "content": f"""
                    Berikan analisis insights bisnis berdasarkan data berikut:
                    
                    {data_summary}
                    
                    Berikan analisis tentang:
                    1. Kualitas layanan secara keseluruhan berdasarkan distribusi rating
                    2. Popularitas bisnis berdasarkan jumlah ulasan
                    3. Rekomendasi strategi untuk meningkatkan rating dan ulasan
                    4. Pola atau tren yang terlihat dari data
                    5. Insight tentang kategori usaha yang performa baik
                    6. Saran improvement untuk bisnis dengan rating rendah
                    
                    Format respons dalam bahasa Indonesia dengan struktur yang jelas dan actionable insights.
                    """
                }
            ],
            model="llama-3.3-70b-versatile",
            stream=False,
        )
        ai_output = chat_completion.choices[0].message.content
    
        return ai_output
    
    except Exception as e:
        print(f"Error AI call: {e}")
        return "Maaf, terjadi kesalahan dalam menganalisis data dengan AI."

@app.route('/')
def main():
    return render_template('index.html')

@app.route('/static/<path:filename>')
def static_files(filename):
    return send_from_directory('static', filename)

@app.route('/analyze', methods=['GET', 'POST'])
def analyze_business():
    global uploaded_data
    
    if request.method == 'POST':
        # Cek apakah file ada dalam request
        if 'csv_file' not in request.files:
            flash('Tidak ada file yang diupload', 'error')
            return redirect(request.url)
        
        file = request.files['csv_file']
        
        # Cek jika user tidak memilih file
        if file.filename == '':
            flash('Silakan pilih file CSV', 'error')
            return redirect(request.url)
        
        # Cek ekstensi file
        if not file.filename.endswith('.csv'):
            flash('Silakan upload file dengan format CSV', 'error')
            return redirect(request.url)
        
        try:
            # Baca file CSV
            df = pd.read_csv(file)
            
            # Simpan data yang diupload untuk pencarian
            uploaded_data = df.copy()
            
            # Analisis data
            analysis_results, error = analyze_business_data(df)
            
            if error:
                flash(error, 'error')
                return redirect(request.url)
            
            # Panggil AI untuk analisis lebih lanjut
            ai_output = ai_call(df, analysis_results)
            
            flash(f'Berhasil memuat {len(df)} data bisnis!', 'success')
            
            return render_template('analyze_business.html', 
                                 results=analysis_results, 
                                 ai_output=ai_output,
                                 analysis_success=True,
                                 total_data=len(df))
            
        except Exception as e:
            flash(f'Error membaca file: {str(e)}', 'error')
            return redirect(request.url)
    
    return render_template('analyze_business.html', 
                         results=None, 
                         analysis_success=False,
                         total_data=0)

@app.route('/close_analysis', methods=['POST'])
def close_analysis():
    """Route untuk menutup hasil analisis dan mereset data"""
    global uploaded_data
    
    # Reset global data
    uploaded_data = None
    
    flash('Analisis telah ditutup. Anda dapat mengupload file baru untuk analisis.', 'info')
    return redirect(url_for('analyze_business'))

@app.route('/get_all_data', methods=['GET'])
def get_all_data():
    global uploaded_data
    
    if uploaded_data is None:
        return jsonify({'error': 'Tidak ada data yang diupload'})
    
    try:
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 50))
        
        total_data = len(uploaded_data)
        total_pages = math.ceil(total_data / per_page)
        
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        
        # Pastikan end_idx tidak melebihi panjang data
        end_idx = min(end_idx, total_data)
        
        data_chunk = uploaded_data.iloc[start_idx:end_idx]
        
        # Konversi ke dictionary dengan handle NaN values
        data_records = []
        for _, row in data_chunk.iterrows():
            record = {}
            for col in uploaded_data.columns:
                value = row[col]
                # Handle NaN values
                if pd.isna(value):
                    record[col] = None
                else:
                    record[col] = value
            data_records.append(record)
        
        return jsonify({
            'success': True,
            'data': data_records,
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total_pages': total_pages,
                'total_data': total_data
            }
        })
    
    except Exception as e:
        print(f"Get all data error: {e}")
        return jsonify({'error': f'Error mengambil data: {str(e)}'})
    
@app.route('/download_results', methods=['GET'])
def download_results():
    global uploaded_data
    
    if uploaded_data is None:
        flash("Tidak ada data yang dianalisis untuk diunduh.", "error")
        return redirect(url_for('analyze_business'))

    try:
        # Jalankan kembali analisis agar sinkron dengan yang ditampilkan
        analysis_results, error = analyze_business_data(uploaded_data)
        if error:
            flash(error, "error")
            return redirect(url_for('analyze_business'))

        # Buat list dict sesuai urutan hasil analisis
        export_data = []

        # Ringkasan utama
        export_data.append({
            "Bagian": "Ringkasan",
            "Keterangan": "Total Bisnis",
            "Nilai": analysis_results['statistics']['total_businesses']
        })
        export_data.append({
            "Bagian": "Ringkasan",
            "Keterangan": "Rata-rata Rating",
            "Nilai": analysis_results['statistics']['avg_rating']
        })
        export_data.append({
            "Bagian": "Ringkasan",
            "Keterangan": "Rata-rata Ulasan",
            "Nilai": analysis_results['statistics']['avg_reviews']
        })
        export_data.append({
            "Bagian": "Ringkasan",
            "Keterangan": "Total Ulasan",
            "Nilai": analysis_results['statistics']['total_reviews']
        })

        # Bisnis dengan performa tertentu
        export_data.append({
            "Bagian": "Bisnis Tertinggi",
            "Keterangan": analysis_results['highest_rated']['nama'],
            "Nilai": f"Rating: {analysis_results['highest_rated']['rating']} | Ulasan: {analysis_results['highest_rated']['jumlah_ulasan']} | Kategori: {analysis_results['highest_rated']['kategori']}"
        })
        export_data.append({
            "Bagian": "Bisnis Ulasan Terbanyak",
            "Keterangan": analysis_results['most_reviewed']['nama'],
            "Nilai": f"Rating: {analysis_results['most_reviewed']['rating']} | Ulasan: {analysis_results['most_reviewed']['jumlah_ulasan']} | Kategori: {analysis_results['most_reviewed']['kategori']}"
        })
        export_data.append({
            "Bagian": "Bisnis Terendah",
            "Keterangan": analysis_results['lowest_rated']['nama'],
            "Nilai": f"Rating: {analysis_results['lowest_rated']['rating']} | Ulasan: {analysis_results['lowest_rated']['jumlah_ulasan']} | Kategori: {analysis_results['lowest_rated']['kategori']}"
        })

        # Top 10 Rating
        for idx, row in enumerate(analysis_results['top_10_rating'], start=1):
            export_data.append({
                "Bagian": "Top 10 Rating",
                "Keterangan": f"{idx}. {row['nama']}",
                "Nilai": f"Rating: {row['rating']} | Ulasan: {row['jumlah_ulasan']} | Kategori: {row.get('kategori_usaha', '-')}"
            })

        # Top 10 Ulasan
        for idx, row in enumerate(analysis_results['top_10_reviews'], start=1):
            export_data.append({
                "Bagian": "Top 10 Ulasan",
                "Keterangan": f"{idx}. {row['nama']}",
                "Nilai": f"Rating: {row['rating']} | Ulasan: {row['jumlah_ulasan']} | Kategori: {row.get('kategori_usaha', '-')}"
            })

        # Convert ke DataFrame
        df_export = pd.DataFrame(export_data)

        # Simpan ke buffer
        buf = io.StringIO()
        df_export.to_csv(buf, index=False, encoding="utf-8-sig")
        buf.seek(0)

        return send_file(
            io.BytesIO(buf.getvalue().encode("utf-8-sig")),
            mimetype="text/csv",
            as_attachment=True,
            download_name=f"hasil_analisis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )

    except Exception as e:
        flash(f"Gagal membuat file CSV: {str(e)}", "error")
        return redirect(url_for('analyze_business'))


if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True)