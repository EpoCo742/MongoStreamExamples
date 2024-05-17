using System;
using System.Collections.Generic;
using System.IO;

class FileSignatureValidator
{
    private static readonly Dictionary<string, byte[]> FileSignatures = new Dictionary<string, byte[]>
    {
        { ".jpg", new byte[] { 0xFF, 0xD8, 0xFF } },
        { ".png", new byte[] { 0x89, 0x50, 0x4E, 0x47 } },
        { ".exe", new byte[] { 0x4D, 0x5A } },
        { ".pdf", new byte[] { 0x25, 0x50, 0x44, 0x46 } }
    };

    public bool ValidateFileSignature(Stream fileStream, string fileExtension)
    {
        try
        {
            fileExtension = fileExtension.ToLower();

            if (!FileSignatures.TryGetValue(fileExtension, out byte[] expectedSignature))
            {
                Console.WriteLine($"No signature information for the extension '{fileExtension}'");
                return false;
            }

            byte[] fileHeader = new byte[expectedSignature.Length];
            if (fileStream.Read(fileHeader, 0, expectedSignature.Length) < expectedSignature.Length)
            {
                return false; // Stream too small to contain expected signature
            }

            for (int i = 0; i < expectedSignature.Length; i++)
            {
                if (fileHeader[i] != expectedSignature[i])
                {
                    return false; // Signature mismatch
                }
            }

            return true; // Signature matched
        }
        catch (Exception ex)
        {
            Console.WriteLine($"An error occurred: {ex.Message}");
            return false;
        }
    }
}

class Program
{
    static void Main()
    {
        string filePath = "path/to/your/file.jpg"; // Change the file path as needed
        string fileExtension = Path.GetExtension(filePath);

        var validator = new FileSignatureValidator();
        using (FileStream fs = new FileStream(filePath, FileMode.Open, FileAccess.Read))
        {
            bool isValid = validator.ValidateFileSignature(fs, fileExtension);
            Console.WriteLine($"The file '{filePath}' has a valid extension: {isValid}");
        }

        // Example usage with a MemoryStream
        byte[] fileBytes = File.ReadAllBytes(filePath);
        using (MemoryStream ms = new MemoryStream(fileBytes))
        {
            bool isValid = validator.ValidateFileSignature(ms, fileExtension);
            Console.WriteLine($"The memory stream file has a valid extension: {isValid}");
        }
    }
}
