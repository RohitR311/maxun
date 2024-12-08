import axios from "axios";
import { useState, useContext, useEffect } from "react";
import { useNavigate, Link } from "react-router-dom";
import { AuthContext } from "../context/auth";
import { Box, Typography, TextField, Button, CircularProgress } from "@mui/material";
import { useGlobalInfoStore } from "../context/globalInfo";
import { apiUrl } from "../apiConfig";
import { useThemeMode } from "../context/theme-provider";

const Register = () => {
  const [form, setForm] = useState({ email: "", password: "" });
  const [loading, setLoading] = useState(false);
  const { notify } = useGlobalInfoStore();
  const { email, password } = form;

  const { state, dispatch } = useContext(AuthContext);
  const { user } = state;
  const { darkMode } = useThemeMode();

  const navigate = useNavigate();

  useEffect(() => {
    if (user) {
      navigate("/");
    }
  }, [user, navigate]);

  const handleChange = (e: any) => {
    const { name, value } = e.target;
    setForm({ ...form, [name]: value });
  };

  const submitForm = async (e: any) => {
    e.preventDefault();
    setLoading(true);
    try {
      const { data } = await axios.post(`${apiUrl}/auth/register`, { email, password });
      console.log(data);
      dispatch({ type: "LOGIN", payload: data });
      notify("success", "Registration Successful!");
      window.localStorage.setItem("user", JSON.stringify(data));
      navigate("/");
    } catch (error: any) {
      notify("error", error.response?.data || "Registration Failed. Please try again.");
      setLoading(false);
    }
  };

  return (
    <Box
      sx={{
        display: "flex",
        justifyContent: "center",
        alignItems: "center",
        maxHeight: "100vh",
        mt: 6,
        padding: 4,
        backgroundColor: darkMode ? "#121212" : "#ffffff",
       
      }}
    >
      <Box
        component="form"
        onSubmit={submitForm}
        sx={{
          textAlign: "center",
          backgroundColor: darkMode ? "#1e1e1e" : "#ffffff",
          color: darkMode ? "#ffffff" : "#333333",
          padding: 6,
          borderRadius: 5,
          boxShadow: "0px 20px 40px rgba(0, 0, 0, 0.2), 0px -5px 10px rgba(0, 0, 0, 0.15)",
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          maxWidth: 400,
          width: "100%",
        }}
      >
        <img
          src="../src/assets/maxunlogo.png"
          alt="logo"
          height={55}
          width={60}
          style={{
            marginBottom: 20,
            borderRadius: "20%",
            alignItems: "center",
          }}
        />
        <Typography variant="h4" gutterBottom>
          Create an Account
        </Typography>
        <TextField
          fullWidth
          label="Email"
          name="email"
          value={email}
          onChange={handleChange}
          margin="normal"
          variant="outlined"
          required
          sx={{
            input: { color: darkMode ? "#ffffff" : "#000000" },
            label: { color: darkMode ? "#bbbbbb" : "#000000" },
            "& .MuiOutlinedInput-root": {
              "& fieldset": { borderColor: darkMode ? "#555555" : "#cccccc" },
              "&:hover fieldset": { borderColor: darkMode ? "#ffffff" : "#000000" },
              "&.Mui-focused fieldset": { borderColor: "#ff33cc" },
            },
          }}
        />
        <TextField
          fullWidth
          label="Password"
          name="password"
          type="password"
          value={password}
          onChange={handleChange}
          margin="normal"
          variant="outlined"
          required
          sx={{
            input: { color: darkMode ? "#ffffff" : "#000000" },
            label: { color: darkMode ? "#bbbbbb" : "#000000" },
            "& .MuiOutlinedInput-root": {
              "& fieldset": { borderColor: darkMode ? "#555555" : "#cccccc" },
              "&:hover fieldset": { borderColor: darkMode ? "#ffffff" : "#000000" },
              "&.Mui-focused fieldset": { borderColor: "#ff33cc" },
            },
          }}
        />
        <Button
          type="submit"
          fullWidth
          variant="contained"
          color="primary"
          sx={{
            mt: 2,
            mb: 2,
            backgroundColor: "#ff33cc",
            "&:hover": {
              backgroundColor:"#e6009e", 
            },
          }}
          disabled={loading || !email || !password}
        >
          {loading ? (
            <>
              <CircularProgress size={20} sx={{ mr: 2, color: "#ffffff" }} />
              Loading
            </>
          ) : (
            "Register"
          )}
        </Button>
        <Typography variant="body2" align="center" sx={{ color: darkMode ? "#ffffff" : "#333333" }}>
          Already have an account?{" "}
          <Link to="/login" style={{ textDecoration: "none", color: "#ff33cc" }}>
            Login
          </Link>
        </Typography>
      </Box>
    </Box>
  );
};

export default Register;
